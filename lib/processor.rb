require 'redis'

class Processor

  LUA_JOIN_LISTS = <<EOS.freeze
while true do
  local val = redis.call('LPOP', KEYS[1])
  if not val then return end
  redis.call('RPUSH', KEYS[2], val)
end
EOS

  EVENTS_MAIN_QUEUE = "events:queue".freeze
  EVENTS_TEMP_QUEUE = "events:dispatching".freeze

  OPERATORS_QUEUE = "operators:queue".freeze
  OPERATORS_TEMP = "operators:processing".freeze
  OPERATORS_KNOWN_SET = "operators:known".freeze

  class Dispatcher
    def initialize(redis)
      @redis = redis
    end

    def dispatch_all(&block)
      while @redis.exists(EVENTS_MAIN_QUEUE) do
        # Process queue safely: move master queue to a temporary one that won't
        # be touched by other clients. If this instance crashes the temporary
        # queue will be merged again to the master (see {Processor#dispatcher}).
        @redis.rename(EVENTS_MAIN_QUEUE, EVENTS_TEMP_QUEUE)
        list = prepare_events(EVENTS_TEMP_QUEUE, &block)

        processing_cnt, abandoned = find_abandoned_operators

        # Enqueue events and delete temporary queue atomically
        @redis.multi do
          enqueue_events(list, processing_cnt, abandoned)
          @redis.del(EVENTS_TEMP_QUEUE)
        end
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

      def enqueue_events(events, processing_operators_cnt, abandoned_operators)
        seen = []

        events.each do |(operator_id, installation_id, data)|
          op_str = operator_id.to_s

          # Keep a set of known operators. On restart/cleanup we need to scan
          # for operator queues abandoned/locked by crashed workers (see below).
          @redis.sadd(OPERATORS_KNOWN_SET, op_str)
          @redis.lpush(OPERATORS_QUEUE, op_str) unless seen.include?(op_str)
          seen << op_str

          @redis.lpush(operator_queue(op_str),
            { 'installation_id' => installation_id, 'data' => data }.to_json)
        end

        abandoned_operators.each do |op_str|
          @redis.lpush(OPERATORS_QUEUE, op_str) unless seen.include?(op_str)
        end
        (1..processing_operators_cnt).each { @redis.rpop(OPERATORS_TEMP) }
      end

      def find_abandoned_operators
        known, processing = @redis.multi do
          @redis.smembers(OPERATORS_KNOWN_SET)
          @redis.lrange(OPERATORS_TEMP, 0, -1)
        end

        [processing.size, processing.select { |x| known.include?(x) }]
      end

      def operator_queue(id)
        "operators:#{id}:events"
      end
  end

  def initialize(redis)
    @redis = redis
  end

  def dispatcher
    return @dispatcher if @dispatcher

    @redis.eval(LUA_JOIN_LISTS, [EVENTS_TEMP_QUEUE, EVENTS_MAIN_QUEUE])
    @dispatcher = Dispatcher.new(@redis)
  end

end
