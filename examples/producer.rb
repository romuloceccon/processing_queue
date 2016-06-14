require 'processing_queue'
require 'json'

redis = Redis.new

loop do
  n = rand(5000)
  redis.multi do
    redis.lpush(ProcessingQueue::INITIAL_QUEUE, [n].to_json)
    redis.incr(ProcessingQueue::EVENTS_COUNTERS_RECEIVED)
  end
  puts(n.to_s)
  Kernel.sleep(0.02)
end
