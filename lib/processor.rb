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
  OPERATORS_KNOWN_SET = "operators:known".freeze

  class Dispatcher
    def initialize(redis)
      @redis = redis
    end

    def dispatch_all(&block)
      while @redis.exists(EVENTS_MAIN_QUEUE) do
        @redis.rename(EVENTS_MAIN_QUEUE, EVENTS_TEMP_QUEUE)
        list = prepare_events(EVENTS_TEMP_QUEUE, &block)

        @redis.multi do
          enqueue_events(list)
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

      def enqueue_events(events)
        seen = []
        events.each do |(operator_id, installation_id, data)|
          op_str = operator_id.to_s

          @redis.sadd(OPERATORS_KNOWN_SET, op_str)
          @redis.lpush(OPERATORS_QUEUE, op_str) unless seen.include?(op_str)
          seen << op_str

          @redis.lpush(operator_queue(op_str),
            { 'installation_id' => installation_id, 'data' => data }.to_json)
        end
      end

      def operator_queue(id)
        "operator:#{id}:events"
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
