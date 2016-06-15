require 'processing_queue'
require 'json'

redis = Redis.new

loop do
  n = rand(5000)
  redis.lpush(ProcessingQueue::INITIAL_QUEUE, [n].to_json)
  puts(n.to_s)
  Kernel.sleep(0.02)
end
