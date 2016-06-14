require 'processor'
require 'json'

processor = Processor.new(Redis.new)
worker = processor.worker
worker.trap!

loop do
  operator = worker.wait_for_operator
  puts("Processing operator %s" % [operator])
  cnt = 0
  worker.process(operator) do |events|
    # begin tran
    events.each do |event|
      cnt += 1
      puts("inst %3d  ev %5d" % [event['object'], event['data'].first])
      Kernel.sleep(0.02 + rand * 0.04)
    end
    puts("Processed: %d events" % cnt)
    Kernel.sleep(0.2)
    # end tran
  end
end
