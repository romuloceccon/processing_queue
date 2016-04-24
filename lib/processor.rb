require 'redis'

class Processor

  LUA_JOIN_LISTS = <<EOS.freeze
while true do
  local val = redis.call('LPOP', KEYS[1])
  if not val then return end
  redis.call('RPUSH', KEYS[2], val)
end
EOS

  class Dispatcher
    def initialize(redis)
      @redis = redis
    end

    def dispatch_all
      @redis.rename("events:queue", "events:dispatching")

      cnt = @redis.llen("events:dispatching")
      arr = (1..cnt).map do |i|
        json = @redis.lindex("events:dispatching", -i)
        data = JSON.parse(json)
        operator_id, installation_id = yield(data)
        [operator_id, installation_id, data]
      end

      @redis.multi do
        seen = []
        arr.each do |(operator_id, installation_id, data)|
          op_str = operator_id.to_s

          @redis.sadd("operators:known", op_str)
          @redis.lpush("operators:queue", op_str) unless seen.include?(op_str)
          seen << op_str

          @redis.lpush("operator:#{op_str}:events",
            { 'installation_id' => installation_id, 'data' => data }.to_json)
        end
        @redis.del("events:dispatching")
      end
    end
  end

  def initialize(redis)
    @redis = redis
  end

  def dispatcher
    return @dispatcher if @dispatcher

    @redis.eval(LUA_JOIN_LISTS, ["events:dispatching", "events:queue"])
    @dispatcher = Dispatcher.new(@redis)
  end

end
