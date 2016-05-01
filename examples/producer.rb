require 'processor'
require 'json'

redis = Redis.new

loop do
  n = rand(5000)
  redis.lpush(Processor::EVENTS_MAIN_QUEUE, [n].to_json)
  redis.incr(Processor::EVENTS_COUNTERS_RECEIVED)
  puts(n.to_s)
  Kernel.sleep(0.05)
end
