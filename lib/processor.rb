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

  EVENTS_MAIN_QUEUE = "events:queue".freeze
  EVENTS_TEMP_QUEUE = "events:dispatching".freeze

  EVENTS_COUNTERS_RECEIVED = "events:counters:received"
  EVENTS_COUNTERS_PROCESSED = "events:counters:processed"

  OPERATORS_QUEUE = "operators:queue".freeze
  OPERATORS_TEMP = "operators:processing".freeze
  OPERATORS_KNOWN_SET = "operators:known".freeze

  class Dispatcher
    def initialize(redis)
      @redis = redis
    end

    def dispatch_all(&block)
      if @redis.exists(EVENTS_MAIN_QUEUE)
        # Process queue safely: move master queue to a temporary one that won't
        # be touched by the workers. If this instance crashes the temporary
        # queue will be merged again to the master (see {Processor#dispatcher}).
        @redis.rename(EVENTS_MAIN_QUEUE, EVENTS_TEMP_QUEUE)
        list = prepare_events(EVENTS_TEMP_QUEUE, &block)
      else
        list = []
      end

      abandoned_operators = find_abandoned_operators

      # Enqueue events and delete temporary queue atomically
      @redis.multi do
        seen_operators = enqueue_events(list)
        enqueue_abandoned_operators(abandoned_operators, seen_operators)
        @redis.del(EVENTS_TEMP_QUEUE)
      end
    end

    private
      def prepare_events(queue_name)
        cnt = @redis.llen(queue_name)

        (1..cnt).map do |i|
          json = @redis.lindex(queue_name, -i)
          data = JSON.parse(json)
          operator_id, installation_id = yield(data)
          [operator_id, installation_id, data]
        end
      end

      def enqueue_events(events)
        result = []

        events.each do |(operator_id, installation_id, data)|
          op_str = operator_id.to_s

          # Keep a set of known operators. On restart/cleanup we need to scan
          # for operator queues abandoned/locked by crashed workers (see below).
          @redis.sadd(OPERATORS_KNOWN_SET, op_str)
          unless result.include?(op_str)
            @redis.lpush(OPERATORS_QUEUE, op_str)
            result << op_str
          end

          @redis.lpush(Processor.operator_queue(op_str),
            { 'installation_id' => installation_id, 'data' => data }.to_json)
        end

        result
      end

      def find_abandoned_operators
        known, processing = @redis.multi do
          @redis.smembers(OPERATORS_KNOWN_SET)
          @redis.lrange(OPERATORS_TEMP, 0, -1)
        end

        keep_list = []
        abandoned = []

        processing.reverse.each do |x|
          keep_list << (locked = @redis.exists(Processor.operator_lock(x)))
          abandoned << x if known.include?(x) && !locked
        end
        [keep_list, abandoned.uniq]
      end

      def enqueue_abandoned_operators(abandoned_operators, ignore_list)
        keep_list, abandoned = abandoned_operators

        abandoned.each do |op_str|
          unless ignore_list.include?(op_str)
            @redis.lpush(OPERATORS_QUEUE, op_str)
          end
        end

        keep_list.each do |keep|
          if keep
            @redis.rpoplpush(OPERATORS_TEMP, OPERATORS_TEMP)
          else
            @redis.rpop(OPERATORS_TEMP)
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

      @handler_flag = proc { |val| @interrupted = val }
      @handler_exit = proc { |val| @interrupted = val; terminate }
      @trap_handler = @handler_exit
    end

    def wait_for_operator
      terminate if @interrupted
      @redis.brpoplpush(OPERATORS_QUEUE, OPERATORS_TEMP)
    end

    def process(operator)
      queue = Processor.operator_queue(operator)
      lock = Processor.operator_lock(operator)

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
        @redis.evalsha(@clean_script, [lock, OPERATORS_KNOWN_SET, queue],
          [@lock_id, operator])
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

  def initialize(redis)
    @redis = redis
    @join_script = @redis.script('LOAD', LUA_JOIN_LISTS)
  end

  def dispatcher
    return @dispatcher if @dispatcher

    @redis.evalsha(@join_script, [EVENTS_TEMP_QUEUE, EVENTS_MAIN_QUEUE])
    @redis.set(EVENTS_COUNTERS_RECEIVED, "0")
    @redis.set(EVENTS_COUNTERS_PROCESSED, "0")
    @dispatcher = Dispatcher.new(@redis)
  end

  def worker
    return Worker.new(@redis)
  end

  def self.operator_queue(id)
    "operators:#{id}:events"
  end

  def self.operator_lock(id)
    "operators:#{id}:lock"
  end
end
