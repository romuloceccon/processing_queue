require 'redis'
require 'json'
require 'securerandom'

# Queue to process events sequentially among multiple workers.
#
# {ProcessingQueue} takes a Redis list with events, classifies each one of them
# into individual queues, and delegates the processing of those events to
# workers, in such a way that an individual queue can be processed by at most
# one worker at any given time.
#
# ## How it works
#
# A single {ProcessingQueue::Dispatcher} is responsible for classifying the
# events into individual queues. It takes an {ProcessingQueue.INITIAL_QUEUE},
# _pre-processes_ each event through the rules given to
# {ProcessingQueue::Dispatcher#dispatch_all}, and places the event in the
# appropriate {ProcessingQueue.queue_events_list}, using the information
# returned by the _pre-processing_. The id of each destination queue will then
# be placed in a {ProcessingQueue.WAITING_QUEUES_LIST} to signal the workers
# that those queue have events available for _processing_.
#
# Multiple {ProcessingQueue::Worker}'s will wait on
# {ProcessingQueue.WAITING_QUEUES_LIST}. Once an item is ready to be processed
# the worker first tries to setup a {ProcessingQueue.queue_lock}. If it fails,
# because the queue is already locked, it waits again for the next one.
# Otherwise, if the lock is successfully granted, it starts processing the
# events, yielding than sequentially to the block given to
# {ProcessingQueue::Worker#process}. Upon completion the lock is released, the
# events removed from the queue, and the worker goes back to the waiting list.
#
# ## Usage
#
# A dispatcher instance is obtained with {ProcessingQueue#dispatcher}. After
# that it should sit in a loop calling
# {ProcessingQueue::Dispatcher#dispatch_all}, which yields every event found to
# the given block. The block should then return a pair where the first item is
# the name of the queue the event should be dispatched to, and the second item
# is an optional object which will be attached to the event. The method returns
# when all events have been dispatched. The script should sleep for some time
# before the next iteration, until enough events have arrived (that's not
# mandatory, but only a recommendation to avoid keeping the cpu busy processing
# a list which is empty most of the time).
#
# Example:
#
#     processing_queue = ProcessingQueue.new(redis_instance)
#
#     dispatcher = processing_queue.dispatcher
#     loop do
#       dispatcher.dispatch_all do |event|
#         queue = find_queue_name(event)
#         object = find_object(event)
#         [queue, object]
#       end
#
#       sleep(10)
#     end
#
# The worker is obtained with {ProcessingQueue#worker}. It's recommended to call
# {ProcessingQueue::Worker#trap!} right after instantiating the object, in order
# to allow graceful termination of the script when it receives INT or TERM
# signals.
#
# The loop starts by calling {ProcessingQueue::Worker#wait_for_queue}, a method
# which will block until a queue is signaled to be ready by
# {ProcessingQueue::Dispatcher}. The name of the queue is returned and that
# should be passed as the only argument to {ProcessingQueue::Worker#process},
# which yields an enumerator with a list of events (by default limited to 100
# items). The enumerator should be used to iterate over the events. The event
# will be in a slightly different format than what arrives in the dispatcher:
# a hash where the original event will be found at the `"data"` key, and the
# second item returned from the dispatch block will be found at the `"object"`
# key.
#
# Note that, unlike the dispatcher, the worker does not yield the event itself,
# but a list of events. That's required so that the block is able to determine
# precisely when the list is finished, allowing, for example, the processing of
# all events in a single database transaction. And it's also important because
# the events will be removed from the queue _only after the block returns
# successfully_. If the script crashes after processing half the events _all_ of
# them will be rescheduled for processing at the next opportunity. On the other
# hand it's also possible to break (successfully) from processing before
# iterating through the whole enumerator: in such case only the events iterated
# through will be removed from the queue.
#
# So the correct pattern is to open a transaction, iterate through all the
# events, and commit the transaction before returning from the block. Example:
#
#     processing_queue = ProcessingQueue.new(redis_instance)
#
#     worker = @processing_queue.worker
#     worker.trap!
#
#     loop do
#       queue = worker.wait_for_queue
#
#       worker.process(queue) do |events|
#         transaction do
#           events.each do |event|
#             object = event['object']
#             original = event['data']
#             process_event(object, original)
#           end
#         end
#       end
#     end
class ProcessingQueue
  LOCK_TIMEOUT = 300_000
  DEFAULT_BATCH_SIZE = 100

  PERF_COUNTER_RESOLUTION = 5  # 5 seconds
  PERF_COUNTER_HISTORY = 900   # 15 minutes

  # @!method LUA_JOIN_LISTS([dispatching_list, events_list])
  # @!scope class
  #
  # Merge dispatching list into events list.
  #
  # All events in `dispatching_list` will be taken in reverse order and pushed
  # back to `events_list`, such that the events (a) appear in the order they
  # were initially found before being moved to `dispatching_list` and (b) are
  # placed _before_ current events in `events_list`.
  #
  # @param [String] dispatching_list Name of temporary event list
  # @param [String] events_list Name of initial event list
  # @return [nil]
  LUA_JOIN_LISTS = <<EOS.freeze
while true do
  local val = redis.call('LPOP', KEYS[1])
  if not val then return end
  redis.call('RPUSH', KEYS[2], val)
end
EOS

  # @!method LUA_CLEAN_AND_UNLOCK([lock, known_queues, suspended_queues, waiting_queues, events_list], [lock_id, queue])
  # @!scope class
  #
  # Cleanup queue lists and release previously acquired lock.
  #
  # `events_list` will be checked for existing events. If there aren't any then
  # `queue` will be removed from the `known_queues` set. Otherwise `queue`
  # will be pushed back to the `waiting_queues` list, unless it appears in the
  # `suspended_queues` set.
  #
  # `lock` and `lock_id` are used to release the lock according to
  # {http://redis.io/topics/distlock#the-redlock-algorithm The Redlock Algorithm}.
  # If the client executing the script does not own `lock` the cleanup will
  # still happen, but `lock` won't be released.
  #
  # @param [String] lock Queue lock name
  # @param [String] known_queues Set with the known queues
  # @param [String] suspended_queues Set with the suspended queues
  # @param [String] waiting_queues List with the waiting queues
  # @param [String] events_list List with the queue events
  # @param [String] lock_id Queue lock unique id
  # @param [String] queue Queue name
  # @return [Integer] 1 if the lock was successfully released; 0 otherwise
  LUA_CLEAN_AND_UNLOCK = <<EOS.freeze
if redis.call('EXISTS', KEYS[5]) == 0 then
  redis.call('SREM', KEYS[2], ARGV[2])
elseif redis.call('SISMEMBER', KEYS[3], ARGV[2]) == 0 then
  redis.call('LPUSH', KEYS[4], ARGV[2])
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
EOS

  # @!method LUA_RESUME_QUEUE([known_queues, suspended_queues, waiting_queues, events_list], [queue])
  # @!scope class
  #
  # Resumes execution of a suspended `queue`.
  #
  # If there are any events on the `events_list` the queue is added to the
  # `waiting_queues` list and the `known_queues` set. If `queue` is found not to
  # be suspended nothing happens.
  #
  # @param [String] known_queues Set with the known queues
  # @param [String] suspended_queues Set with the suspended queues
  # @param [String] waiting_queues List with the waiting queues
  # @param [String] events_list List with the queue events
  # @param [String] queue Queue name
  # @return [Integer] Always 0 (zero)
  LUA_RESUME_QUEUE = <<EOS.freeze
local queue = ARGV[1]
if redis.call('SISMEMBER', KEYS[2], queue) == 1 then
  if redis.call('EXISTS', KEYS[4]) == 1 then
    redis.call('LPUSH', KEYS[3], queue)
    redis.call('SADD', KEYS[1], queue)
  end
  redis.call('SREM', KEYS[2], queue)
end
return 0
EOS

  # @!method LUA_INC_PERF_COUNTER([perf_counter], [count, ttl])
  # @!scope class
  #
  # Increments performance counter setting a TTL.
  #
  # The performance counter `perf_counter` is incremented by `count` using
  # Redis's `INCRBYFLOAT`. If the key does not exist before the call it's set to
  # expire after `ttl` seconds.
  #
  # @param [String] perf_counter Performance counter key name
  # @param [Float] count Amount by which to increment the performance counter
  # @param [Integer] ttl Time-to-live in seconds
  # @return [nil]
  LUA_INC_PERF_COUNTER = <<EOS.freeze
local exists = redis.call("EXISTS", KEYS[1]) == 1
redis.call("INCRBYFLOAT", KEYS[1], ARGV[1])
if not exists then redis.call("EXPIRE", KEYS[1], ARGV[2]) end
EOS

  INITIAL_QUEUE = "events:queue".freeze
  DISPATCHING_EVENTS_LIST = "events:dispatching".freeze

  EVENTS_COUNTERS_DISPATCHED = "events:counters:dispatched".freeze
  EVENTS_COUNTERS_PROCESSED = "events:counters:processed".freeze

  WAITING_QUEUES_LIST = "queues:waiting".freeze
  PROCESSING_QUEUES_LIST = "queues:processing".freeze
  KNOWN_QUEUES_SET = "queues:known".freeze
  SUSPENDED_QUEUES_SET = "queues:suspended".freeze

  # Pre-processor for classifying events and cleaning up abandoned queues.
  #
  # A single instance of {Dispatcher} should be used to classify events arriving
  # at {ProcessingQueue.INITIAL_QUEUE} and place them at the appropriate
  # {ProcessingQueue.queue_events_list}.
  #
  # It has a safety mechanism in case of a sudden crash: before starting the
  # actual dispatch the list being pre-processed is moved to a temporary one.
  # If no error occurs the temporary list is then removed; otherwise, upon
  # restart, the dispatcher checks for an existing temporary list and merges it
  # into the {ProcessingQueue.INITIAL_QUEUE} before starting the actual dispatch
  # loop.
  #
  # Another important feature is the clean-up of abandoned worker queues. If a
  # worker crashes it's going to leave o Redis lock behind, preventing other
  # workers to work on the same queue. At every iteration the dispatcher will
  # check for expired locks and reschedule the corresponding queue so that
  # another worker can work on it.
  #
  # See {ProcessingQueue} for usage example.
  class Dispatcher
    # Initializes the dispatcher. Used internally by
    # {ProcessingQueue#dispatcher}.
    #
    # @param [Object] redis Redis instance
    def initialize(redis)
      @redis = redis
    end

    # Dispatch all events once.
    #
    # All events will be yielded in sequence to the block given, in whatever
    # format they arrived first at {ProcessingQueue.INITIAL_QUEUE}. The result
    # of the block should be a pair where the first item is the name of a queue
    # where the event should be put in, and the second item is a
    # (json-serializable) object which will be attached to the event and
    # restored when the actual processing occurs. Dispatched events belonging to
    # the same queue are guaranteed to remain in the exact order (relative to
    # each other) they were when they arrived at
    # {ProcessingQueue.INITIAL_QUEUE}.
    #
    # ## Internals
    #
    # Dispatching starts by moving an existing {ProcessingQueue.INITIAL_QUEUE}
    # to a {ProcessingQueue.DISPATCHING_EVENTS_LIST}. They are then yielded in
    # sequence to the block given to get their queue names and attached objects.
    #
    # {ProcessingQueue.KNOWN_QUEUES_SET} and
    # {ProcessingQueue.PROCESSING_QUEUES_LIST} will be checked to find out
    # whether any worker has crashed, leaving a lock behind: if the queue name
    # appears in _both_ lists and no lock exists (meaning the corresponding lock
    # key has expired) it's considered to be abandoned.
    #
    # Next, an _atomic_ operation starts, i.e. it's not suscetible to any kind
    # of race condition with the workers:
    #
    # * for each event:
    #   * the queue name is added to the {ProcessingQueue.KNOWN_QUEUES_SET}
    #   * the queue name is appended to the
    #     {ProcessingQueue.WAITING_QUEUES_LIST}
    #   * the event is appended to its {ProcessingQueue.queue_events_list}
    # * the names of abandoned queues are appended to
    #   {ProcessingQueue.WAITING_QUEUES_LIST}
    # * names of empty queues _which are not currently locked_ are removed from
    #   {ProcessingQueue.PROCESSING_QUEUES_LIST}
    # * temporary list {ProcessingQueue.DISPATCHING_EVENTS_LIST} is removed
    #
    # Note that, because of the nature of Redis "multi" transactions, we cannot
    # retrieve values after the transaction starts. Therefore, we are limited to
    # only _send_ commands. That affects how abandonded queues are managed,
    # because we must avoid the race condition which exists between starting the
    # check and starting the transaction when events are pushed.
    #
    # The dispatcher is the only process which _removes_ items from
    # {ProcessingQueue.PROCESSING_QUEUES_LIST}, while the workers can only
    # _append_ them to it. We rely on that to build a "keep_list" of the queue
    # names, which are simply a series of instructions about what to do with
    # every queue name: either remove or reappend. At the end of the "multi"
    # transaction we "apply" such keep_list. If the instruction says "remove"
    # the item is just popped with `RPOP`. If instead it says "reappend" the
    # item is popped and pushed again with `RPOPLPUSH`. That way we can just
    # "blindly" apply the operations without rechecking the contents of
    # {ProcessingQueue.PROCESSING_QUEUES_LIST}. If an item is appended by the
    # worker in the middle of the apply operation it won't be affected, except
    # by the fact that `RPOPLPUSH`'ed items will appear _after_ the newly
    # appended item.
    #
    # The transaction guarantees that events are removed from the
    # {ProcessingQueue.DISPATCHING_EVENTS_LIST} only after they have been
    # successfully appended to their respective queues. If the process crashes
    # during dispatch it's going to leave a non-empty
    # {ProcessingQueue.DISPATCHING_EVENTS_LIST} behind. Upon restart
    # {ProcessingQueue#dispatcher} will look for it and merge it back with
    # {ProcessingQueue.INITIAL_QUEUE}.
    #
    # If some queue was suspended by {ProcessingQueue#suspend_queue} everything
    # will continue to work as before, except such queue name won't ever be
    # appended to {ProcessingQueue.WAITING_QUEUES_LIST} (thus not becoming
    # available for any worker).
    def dispatch_all(&block)
      if @redis.exists(INITIAL_QUEUE)
        # Process queue safely: move master queue to a temporary one that won't
        # be touched by the workers. If this instance crashes the temporary
        # queue will be merged again to the master (see {ProcessingQueue#dispatcher}).
        @redis.rename(INITIAL_QUEUE, DISPATCHING_EVENTS_LIST)
        list = prepare_events(DISPATCHING_EVENTS_LIST, &block)
      else
        list = []
      end

      abandoned_queues = find_abandoned_queues

      transaction do |suspended|
        # Enqueue events and delete temporary queue atomically
        seen_queues = enqueue_events(list, suspended) + suspended
        enqueue_abandoned_queues(abandoned_queues, seen_queues)
        @redis.del(DISPATCHING_EVENTS_LIST)
      end
    end

    # Append event directly to its {ProcessingQueue.queue_events_list}.
    #
    # One can use {#post} to append an event whose queue_id and object are known
    # directly to its {ProcessingQueue.queue_events_list}. That avoids the
    # overhead of pushing the event into the {ProcessingQueue.INITIAL_QUEUE} and
    # parsing it later through the dispatcher.
    #
    # Of course it's only available inside the application dispatching and/or
    # processing the events, but is useful in scenarios with "out-of-band"
    # events (e.g. the dispatcher wants to post additional events based on the
    # sequence of events posted to {ProcessingQueue.INITIAL_QUEUE}).
    #
    # @param [Object] data The event data
    # @param [String] queue_id Queue name
    # @param [Object] object Object to attach to event (see {#dispatch_all})
    # @return [nil]
    def post(data, queue_id, object=nil)
      transaction do |suspended|
        list = [[queue_id, object, data]]
        enqueue_events(list, suspended)
      end
    end

    private

    def prepare_events(list)
      cnt = @redis.llen(list)

      (1..cnt).map do |i|
        json = @redis.lindex(list, -i)
        data = JSON.parse(json)
        queue_id, object = yield(data)
        [queue_id, object, data]
      end
    end

    def enqueue_events(events, suspended_queues)
      result = []

      events.each do |(queue_id, object, data)|
        id_str = queue_id.to_s

        # Keep a set of known queues. On restart/cleanup we need to scan for
        # queues abandoned/locked by crashed workers (see below).
        @redis.sadd(KNOWN_QUEUES_SET, id_str)
        unless result.include?(id_str) || suspended_queues.include?(id_str)
          @redis.lpush(WAITING_QUEUES_LIST, id_str)
          result << id_str
        end

        hsh = {}
        hsh['data'] = data
        hsh['object'] = object if object
        @redis.lpush(ProcessingQueue.queue_events_list(id_str), hsh.to_json)
        @redis.incr(EVENTS_COUNTERS_DISPATCHED)
      end

      result
    end

    def find_abandoned_queues
      known, processing = @redis.multi do
        @redis.smembers(KNOWN_QUEUES_SET)
        @redis.lrange(PROCESSING_QUEUES_LIST, 0, -1)
      end

      keep_list = []
      abandoned = []

      processing.reverse.each do |x|
        keep_list << (locked = @redis.exists(ProcessingQueue.queue_lock(x)))
        abandoned << x if known.include?(x) && !locked
      end
      [keep_list, abandoned.uniq]
    end

    def enqueue_abandoned_queues(abandoned_queues, ignore_list)
      keep_list, abandoned = abandoned_queues

      abandoned.each do |op_str|
        unless ignore_list.include?(op_str)
          @redis.lpush(WAITING_QUEUES_LIST, op_str)
        end
      end

      keep_list.each do |keep|
        if keep
          @redis.rpoplpush(PROCESSING_QUEUES_LIST, PROCESSING_QUEUES_LIST)
        else
          @redis.rpop(PROCESSING_QUEUES_LIST)
        end
      end
    end

    def transaction(&block)
      # Retry until multi succeeds
      loop do
        @redis.watch(SUSPENDED_QUEUES_SET)
        suspended = @redis.smembers(SUSPENDED_QUEUES_SET)
        break if @redis.multi { yield(suspended) }
      end
    end
  end

  # Serial event processor.
  #
  # One or more {Worker} instances (one per process) should be used to actually
  # process the events which were previously classified by the {Dispatcher}.
  #
  # See {ProcessingQueue} for usage example.
  class Worker
    # Initializes the {Worker}. Used internally by {ProcessingQueue#worker}.
    #
    # It's possible to control the batch size by passing the option
    # `:max_batch_size` in the second argument. By default it's set to
    # {ProcessingQueue.DEFAULT_BATCH_SIZE}.
    #
    # @param [Object] redis Redis instance
    # @param [Hash] options Options hash
    def initialize(redis, options)
      @redis = redis
      @lock_id = "#{SecureRandom.uuid}/#{Process.pid}"
      @max_batch_size = options[:max_batch_size] || DEFAULT_BATCH_SIZE
      @clean_script = @redis.script('LOAD', LUA_CLEAN_AND_UNLOCK)
      @inc_counter_script = @redis.script('LOAD', LUA_INC_PERF_COUNTER)
      @interrupted = nil

      @handler_flag = proc { |val| @interrupted = val }
      @handler_exit = proc { |val| @interrupted = val; terminate }
      @trap_handler = @handler_exit
    end

    # Waits for the next queue.
    #
    # Block while no queue is available for processing. When a queue becomes
    # available the queue name is returned and also appended to
    # {ProcessingQueue.PROCESSING_QUEUES_LIST}. That way, if the worker crashes
    # just after BRPOPLPUSH executes, the dispatcher will see the list as
    # abandoned and reschedule it.
    #
    # @return [String] queue name
    def wait_for_queue
      terminate if @interrupted
      @redis.brpoplpush(WAITING_QUEUES_LIST, PROCESSING_QUEUES_LIST)
    end

    # Processes events from the given queue.
    #
    # The queue name passed as `queue_id`, returned by {#wait_for_queue}, will
    # be processed through the block given. {#process} yields and enumerator to
    # the block, and that enumerator should be iterated through to actually get
    # to the individual events. The event format is:
    #
    #     { "object" => <object>, "data" => <data> }
    #
    # where `<object>` is the object returned as the second item of the block
    # given to {Dispatcher#dispatch_all}, and `<data>` is the actual event as it
    # arrived in {ProcessingQueue.INITIAL_QUEUE} before going through the
    # dispatcher.
    #
    # No more than {ProcessingQueue.DEFAULT_BATCH_SIZE} (or the value given to
    # the constructor) will be processed in a single call to {#process}. That
    # feature is important when processing events inside database transactions,
    # where holding locks for too long could be harmful to the application.
    #
    # Running through the enumerator until the end successfully will remove the
    # events from the queue definitely, and release the worker to work in
    # another queue (or even the same queue if the batch was limited by the
    # configured batch size and no other queue was scheduled).
    #
    # An exception raised inside the block, even if most (or all) events have
    # been processed, has the effect of aborting the whole batch. The queue will
    # leave the lock _unreleased_, and the dispatcher will see it as abandoned
    # after its TTL expires. That's on purpose, to prevent a problematic batch
    # from making all workers raise exceptions endlessly in a busy (or short)
    # loop.
    #
    # As a third option it's also possible to interrupt the processing before
    # the end of the enumerator, by `break`ing from its block. The events
    # processed so far will then be removed from the queue and the remaining
    # ones left to be processed to the next available worker. That's useful, for
    # example, to shutdown the worker gracefully without waiting for longer
    # transactions to complete (or, in case the transaction is almost finished,
    # rolling back previously processed events).
    #
    # ## Internals
    #
    # First the worker tries to set an exclusive {ProcessingQueue.queue_lock} on
    # the queue. If it fails, because another worker is holding it, {#process}
    # returns.
    #
    # Each event in the batch is then processed through the enumerator. After
    # that a single Redis "multi" transaction removes exactly the number of
    # events processed. That's necessary because (a) the dispatcher may append
    # more events to the queue while {#process} is running, and (b) if an
    # exception is raised after processing some events, the whole batch should
    # be kept untouched to be processed again later.
    #
    # {#process} finishes cleaning-up the queue and releasing the lock by
    # calling {ProcessingQueue.LUA_CLEAN_AND_UNLOCK}.
    #
    # ### Performance counters
    #
    # Measuring performance is not trivial. To allow for fast and easy check of
    # current and past worker loads the performance counters have a "timeline"
    # divided into slots. Each slot stores information about the number of
    # events processed and the total time spent actually processing them (as
    # opposed to waiting for available queues). The default slot is 5-seconds
    # wide. So, to calculate the performance during the last minute, one may
    # just operate on all counters from the last 12 slots.
    #
    # The actual measure is done approximately. The time taken to process each
    # event is taken individually and stored at the current slot. When the
    # processing of a single event splits across two (or more) slots we assume,
    # to simplify, that the whole event was processed in the last slot. That's a
    # reasonable approximation when events take just a fraction of the slot
    # width to process.
    #
    # @param [String] queue_id The queue name
    # @return [Boolean] `false` if lock could not be acquired; true otherwise
    def process(queue_id)
      queue = ProcessingQueue.queue_events_list(queue_id)
      lock = ProcessingQueue.queue_lock(queue_id)

      @trap_handler = @handler_flag
      begin
        unless @redis.set(lock, @lock_id, nx: true, px: LOCK_TIMEOUT)
          return false
        end

        performance = Performance.new(@redis, @inc_counter_script)

        cnt = 0
        enum = Enumerator.new do |y|
          while !@interrupted && cnt < @max_batch_size &&
              event = @redis.lindex(queue, -(cnt + 1)) do
            cnt += 1
            performance.store(1)
            y << JSON.parse(event)
          end
        end
        yield(enum)
        performance.store(0)

        @redis.multi do
          (1..cnt).each do
            @redis.rpop(queue)
            @redis.incr(EVENTS_COUNTERS_PROCESSED)
          end
        end

        @redis.evalsha(
          @clean_script,
          [lock, KNOWN_QUEUES_SET, SUSPENDED_QUEUES_SET, WAITING_QUEUES_LIST, queue],
          [@lock_id, queue_id])
        true
      ensure
        @trap_handler = @handler_exit
      end
    end

    # Sets up signal traps to allow clean shutdown.
    #
    # Calling {#trap!} is important to allow the worker to shutdown gracefully
    # in case a SIGTERM or SIGINT is delivered to the application. It changes
    # the owner process behavior in two ways:
    #
    # 1. If _outside_ of {#process}, `Kernel.exit` is called immediatelly;
    # 2. Otherwise, an `@interrupted` flag is set and the whole processing stops
    #    when the _current_ event finishes processing. The lock is released so
    #    that another worker processes the remaining events, and `Kernel.exit`
    #    is called.
    #
    # The value passed to exit follows Bash (and other shells) convention, i.e.
    # the process returns with a status code equal to 128 + <signal number>.
    def trap!
      return if @trapped
      @trapped = true
      Signal.trap('INT') { @trap_handler.call(130) }  # 128 + SIGINT
      Signal.trap('TERM') { @trap_handler.call(143) } # 128 + SIGTERM
    end

    private

    def terminate
      exit(@interrupted)
    end
  end

  # Helper to store performance counters. Used internally by {Worker}.
  class Performance
    def initialize(redis, script)
      @redis = redis
      @script = script
      @previous = get_time
    end

    # Add time and count to performance counters.
    #
    # Current slot id is determined by dividing the current time (as given by
    # the monotonic process clock) by the slot width. `count` and the time since
    # the last call (or object initialization) is then added to the current
    # slot.
    def store(count)
      t = get_time
      diff, @previous = t - @previous, t

      slot = (t / PERF_COUNTER_RESOLUTION).to_i
      time_counter = "events:counters:#{slot}:time"
      cnt_counter = "events:counters:#{slot}:count"

      update_counter(time_counter, diff)
      update_counter(cnt_counter, count)
    end

    private

    def get_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def update_counter(counter, val)
      return if val <= 0
      @redis.evalsha(@script, [counter], [val, PERF_COUNTER_HISTORY])
    end
  end

  class Statistics
    class Queue
      attr_reader :name, :count, :locked_by, :ttl

      def initialize(redis, queue_id, suspended_queues, waiting_queues,
          processing_queues)
        @name = queue_id
        @count = redis.llen(ProcessingQueue.queue_events_list(queue_id))
        @suspended = suspended_queues.include?(queue_id)
        @queued = waiting_queues.include?(queue_id)
        @processing = processing_queues.include?(queue_id)
        @locked_by = @ttl = nil

        lock_name = ProcessingQueue.queue_lock(queue_id)
        if lock = redis.get(lock_name)
          @locked_by = lock.split('/').last
          @ttl = redis.pttl(lock_name)
        end
      end

      def locked?
        !!@locked_by
      end

      def queued?
        @queued
      end

      def suspended?
        @suspended
      end

      def taken?
        @processing
      end

      def <=>(other)
        return suspended? ? -1 : 1 if suspended? != other.suspended?
        return locked? ? -1 : 1 if locked? != other.locked?
        return other.count <=> count if count != other.count
        return name <=> other.name
      end
    end

    class Counter
      attr_reader :count, :count_per_min, :time_busy

      def initialize(secs, values_count, values_time)
        @count = sum_recent_counters(values_count, secs)
        @count_per_min = @count / (secs / 60)
        @time_busy = sum_recent_counters(values_time, secs) / secs
      end

      private

      def sum_recent_counters(arr, secs)
        arr.slice(0, secs / PERF_COUNTER_RESOLUTION).
          inject(0) { |acc, x| acc + x }
      end
    end

    attr_reader :received_count, :dispatched_count, :processed_count
    attr_reader :waiting_count, :queue_length
    attr_reader :queues
    attr_reader :counters

    def initialize(redis)
      @redis = redis
      update
    end

    def update
      disp_cnt, proc_cnt, q_len, q_known, q_susp, q_waiting, q_proc = @redis.multi do
        @redis.get(EVENTS_COUNTERS_DISPATCHED)
        @redis.get(EVENTS_COUNTERS_PROCESSED)
        @redis.llen(INITIAL_QUEUE)

        @redis.smembers(KNOWN_QUEUES_SET)
        @redis.smembers(SUSPENDED_QUEUES_SET)
        @redis.lrange(WAITING_QUEUES_LIST, 0, -1)
        @redis.lrange(PROCESSING_QUEUES_LIST, 0, -1)
      end

      @dispatched_count, @processed_count, @queue_length =
        disp_cnt.to_i, proc_cnt.to_i, q_len.to_i

      @received_count = @dispatched_count + @queue_length

      q_all = (q_known + q_susp + q_waiting + q_proc).uniq
      @queues = q_all.map do |q_id|
        Queue.new(@redis, q_id, q_susp, q_waiting, q_proc)
      end
      @queues.sort!

      @waiting_count = @queues.inject(0) { |r, obj| r + obj.count }

      res = PERF_COUNTER_RESOLUTION
      t = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      cur_slot = (t / res).to_i
      slot_cnt = (900 / res).to_i

      values = @redis.multi do
        (1..slot_cnt).each do |s|
          @redis.get("events:counters:#{cur_slot - s}:count")
        end
        (1..slot_cnt).each do |s|
          @redis.get("events:counters:#{cur_slot - s}:time")
        end
      end

      values_count = values.slice(0, slot_cnt).map(&:to_i)
      values_time = values.slice(slot_cnt, 2 * slot_cnt).map(&:to_f)

      @counters = [
        Counter.new(60, values_count, values_time),
        Counter.new(300, values_count, values_time),
        Counter.new(900, values_count, values_time)]
    end
  end

  # Initialize the {ProcessingQueue}.
  #
  # @param [Object] redis Redis instance which will be passed to the worker and
  #   dispatcher
  def initialize(redis)
    @redis = redis
    @dispatcher = nil
    @join_script = @redis.script('LOAD', LUA_JOIN_LISTS)
  end

  # Creates a dispatcher.
  #
  # Creates an instance of {Dispatcher} using the Redis instance given to the
  # constructor.
  #
  # @return [Dispatcher]
  def dispatcher
    return @dispatcher if @dispatcher

    @redis.evalsha(@join_script, [DISPATCHING_EVENTS_LIST, INITIAL_QUEUE])
    @dispatcher = Dispatcher.new(@redis)
  end

  # Resumes execution of a queue suspended by {#suspend_queue}.
  def resume_queue(queue)
    @redis.eval(LUA_RESUME_QUEUE, [KNOWN_QUEUES_SET, SUSPENDED_QUEUES_SET,
      WAITING_QUEUES_LIST, ProcessingQueue.queue_events_list(queue)], [queue])
  end

  # Suspend execution of a queue.
  #
  # A queue can be suspended so that no worker ever acquires its lock. It's
  # useful for debugging purposes, for example, when a problematic queue in a
  # production system keeps crashing the workers. Note that suspending a queue
  # does not release any currently held lock. It merely signals the dispatcher
  # that the queue should never be scheduled again.
  #
  # To successfully operate on a suspended queue create an instance of {Worker}
  # and call {Worker#process} until it succeeds:
  #
  #     processing_queue = ProcessingQueue.new(redis_instance)
  #     processing_queue.suspend_queue('my-queue')
  #
  #     worker = @processing_queue.worker
  #     worker.trap!
  #
  #     loop do
  #       break if worker.process('my-queue') do |events|
  #         process events ...
  #       end
  #
  #       sleep(1)
  #     end
  #
  #     processing_queue.resume_queue('my-queue')
  def suspend_queue(queue)
    @redis.multi do
      @redis.sadd(SUSPENDED_QUEUES_SET, queue)
      @redis.lrem(WAITING_QUEUES_LIST, 0, queue)
    end
  end

  # Creates a worker.
  #
  # Creates an instance of {Worker}. `options` will be passed to {Worker}'s
  # constructor.
  #
  # @param [Hash] options
  # @return [Worker]
  def worker(options={})
    return Worker.new(@redis, options)
  end

  # Returns full name of queue with given id.
  #
  # @param [String] id Queue id
  # @return [String]
  def self.queue_events_list(id)
    "queues:#{id}:events"
  end

  # Returns full name of lock with given queue id.
  #
  # @param [String] id Queue id
  # @return [String]
  def self.queue_lock(id)
    "queues:#{id}:lock"
  end
end
