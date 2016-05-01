require 'processor'
require 'json'

processor = Processor.new(Redis.new)
worker = processor.worker
worker.trap!

loop do
  operator = worker.wait_for_operator
  puts("Processing operator %s" % [operator])
  worker.process(operator) do |event|
    puts("inst %3d  ev %5d" % [event['installation_id'], event['data'].first])
    Kernel.sleep(0.2 + rand * 0.8)
  end
end
