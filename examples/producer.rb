require 'processor'
require 'json'

redis = Redis.new

loop do
  n = rand(5000)
  redis.multi do
    redis.lpush(Processor::INITIAL_QUEUE, [n].to_json)
    redis.incr(Processor::EVENTS_COUNTERS_RECEIVED)
  end
  puts(n.to_s)
  Kernel.sleep(0.02)
end
