require 'redis'

class Processor

  LUA_JOIN_LISTS = <<EOS.freeze
while true do
  local val = redis.call('LPOP', KEYS[1])
  if not val then return end
  redis.call('RPUSH', KEYS[2], val)
end
EOS

  def initialize(redis)
    @redis = redis
  end

  def dispatcher
    return @dispatcher if @dispatcher

    @redis.eval(LUA_JOIN_LISTS, ["events:dispatching", "events:queue"])
    @dispatcher = true
  end

end
