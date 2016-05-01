require 'processor'

processor = Processor.new(Redis.new)
dispatcher = processor.dispatcher

loop do
  dispatcher.dispatch_all do |event|
    install_id = event.first % 131
    operator_id = install_id % 11
    puts("op: %2d  inst %3d  ev %5d" % [operator_id, install_id, event.first])
    [operator_id, install_id]
  end

  Kernel.sleep(10)
end
