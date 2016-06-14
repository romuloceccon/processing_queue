require 'redis'
require 'json'
require 'securerandom'

class Processor
  LOCK_TIMEOUT = 300_000
  MAX_BATCH_SIZE = 100

  PERF_COUNTER_RESOLUTION = 5  # 5 seconds
  PERF_COUNTER_HISTORY = 900   # 15 minutes

  LUA_JOIN_LISTS = <<EOS.freeze
while true do
  local val = redis.call('LPOP', KEYS[1])
  if not val then return end
  redis.call('RPUSH', KEYS[2], val)
end
EOS

  LUA_CLEAN_AND_UNLOCK = <<EOS.freeze
if redis.call('EXISTS', KEYS[3]) == 0 then
  redis.call('SREM', KEYS[2], ARGV[2])
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
EOS

  LUA_INC_PERF_COUNTER = <<EOS.freeze
local exists = redis.call("EXISTS", KEYS[1]) == 1
redis.call("INCRBYFLOAT", KEYS[1], ARGV[1])
if not exists then redis.call("EXPIRE", KEYS[1], ARGV[2]) end
EOS

  INITIAL_QUEUE = "events:queue".freeze
  DISPATCHING_EVENTS_LIST = "events:dispatching".freeze

  EVENTS_COUNTERS_RECEIVED = "events:counters:received"
  EVENTS_COUNTERS_PROCESSED = "events:counters:processed"

  WAITING_QUEUES_LIST = "queues:waiting".freeze
  PROCESSING_QUEUES_LIST = "queues:processing".freeze
  KNOWN_QUEUES_SET = "queues:known".freeze

  class Dispatcher
    def initialize(redis)
      @redis = redis
    end

    def dispatch_all(&block)
      if @redis.exists(INITIAL_QUEUE)
        # Process queue safely: move master queue to a temporary one that won't
        # be touched by the workers. If this instance crashes the temporary
        # queue will be merged again to the master (see {Processor#dispatcher}).
        @redis.rename(INITIAL_QUEUE, DISPATCHING_EVENTS_LIST)
        list = prepare_events(DISPATCHING_EVENTS_LIST, &block)
      else
        list = []
      end

      abandoned_queues = find_abandoned_queues

      # Enqueue events and delete temporary queue atomically
      @redis.multi do
        seen_queues = enqueue_events(list)
        enqueue_abandoned_queues(abandoned_queues, seen_queues)
        @redis.del(DISPATCHING_EVENTS_LIST)
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

      def enqueue_events(events)
        result = []

        events.each do |(queue_id, object, data)|
          id_str = queue_id.to_s

          # Keep a set of known queues. On restart/cleanup we need to scan for
          # queues abandoned/locked by crashed workers (see below).
          @redis.sadd(KNOWN_QUEUES_SET, id_str)
          unless result.include?(id_str)
            @redis.lpush(WAITING_QUEUES_LIST, id_str)
            result << id_str
          end

          hsh = {}
          hsh['data'] = data
          hsh['object'] = object if object
          @redis.lpush(Processor.queue_events_list(id_str), hsh.to_json)
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
          keep_list << (locked = @redis.exists(Processor.queue_lock(x)))
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
  end

  class Worker
    def initialize(redis)
      @redis = redis
      @lock_id = "#{SecureRandom.uuid}/#{Process.pid}"
      @clean_script = @redis.script('LOAD', LUA_CLEAN_AND_UNLOCK)
      @inc_counter_script = @redis.script('LOAD', LUA_INC_PERF_COUNTER)
      @interrupted = nil

      @handler_flag = proc { |val| @interrupted = val }
      @handler_exit = proc { |val| @interrupted = val; terminate }
      @trap_handler = @handler_exit
    end

    def wait_for_queue
      terminate if @interrupted
      @redis.brpoplpush(WAITING_QUEUES_LIST, PROCESSING_QUEUES_LIST)
    end

    def process(queue_id)
      queue = Processor.queue_events_list(queue_id)
      lock = Processor.queue_lock(queue_id)

      @trap_handler = @handler_flag
      begin
        return unless @redis.set(lock, @lock_id, nx: true, px: LOCK_TIMEOUT)

        performance = Performance.new(@redis, @inc_counter_script)

        cnt = 0
        enum = Enumerator.new do |y|
          while !@interrupted && cnt < MAX_BATCH_SIZE &&
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

        # Ver [The Redlock Algorithm](http://redis.io/commands/setnx).
        @redis.evalsha(@clean_script, [lock, KNOWN_QUEUES_SET, queue],
          [@lock_id, queue_id])
      ensure
        @trap_handler = @handler_exit
      end
    end

    def trap!
      return if @trapped
      @trapped = true
      Signal.trap('INT') { @trap_handler.call(130) }
      Signal.trap('TERM') { @trap_handler.call(143) }
    end

    private
      def terminate
        exit(@interrupted)
      end
  end

  class Performance
    def initialize(redis, script)
      @redis = redis
      @script = script
      @previous = get_time
    end

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

      def initialize(redis, queue_id, waiting_queues, processing_queues)
        @name = queue_id
        @count = redis.llen(Processor.queue_events_list(queue_id))
        @queued = waiting_queues.include?(queue_id)
        @processing = processing_queues.include?(queue_id)
        @locked_by = @ttl = nil

        lock_name = Processor.queue_lock(queue_id)
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

      def taken?
        @processing
      end

      def <=>(other)
        return locked? ? -1 : 1 if locked? ^ other.locked?
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
          arr.slice(0, secs / Processor::PERF_COUNTER_RESOLUTION).
            inject(0) { |acc, x| acc + x }
        end
    end

    attr_reader :received_count, :processed_count, :waiting_count, :queue_length
    attr_reader :queues
    attr_reader :counters

    def initialize(redis)
      @redis = redis
      update
    end

    def update
      @received_count = @redis.get('events:counters:received').to_i
      @processed_count = @redis.get('events:counters:processed').to_i
      @queue_length = @redis.llen(Processor::INITIAL_QUEUE).to_i

      q_known, q_waiting, q_processing = @redis.multi do
        @redis.smembers(Processor::KNOWN_QUEUES_SET)
        @redis.lrange(Processor::WAITING_QUEUES_LIST, 0, -1)
        @redis.lrange(Processor::PROCESSING_QUEUES_LIST, 0, -1)
      end

      q_all = (q_known + q_waiting + q_processing).uniq
      @queues = q_all.map do |q_id|
        Queue.new(@redis, q_id, q_waiting, q_processing)
      end
      @queues.sort!

      @waiting_count = @queues.inject(0) { |r, obj| r + obj.count }

      res = Processor::PERF_COUNTER_RESOLUTION
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

  def initialize(redis)
    @redis = redis
    @dispatcher = nil
    @join_script = @redis.script('LOAD', LUA_JOIN_LISTS)
  end

  def dispatcher
    return @dispatcher if @dispatcher

    @redis.evalsha(@join_script, [DISPATCHING_EVENTS_LIST, INITIAL_QUEUE])
    @redis.set(EVENTS_COUNTERS_RECEIVED, "0")
    @redis.set(EVENTS_COUNTERS_PROCESSED, "0")
    @dispatcher = Dispatcher.new(@redis)
  end

  def worker
    return Worker.new(@redis)
  end

  def self.queue_events_list(id)
    "queues:#{id}:events"
  end

  def self.queue_lock(id)
    "queues:#{id}:lock"
  end
end
