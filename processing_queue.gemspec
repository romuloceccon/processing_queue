Gem::Specification.new do |s|
  s.name        = 'processing_queue'
  s.version     = '0.3.4'
  s.date        = '2017-08-14'
  s.summary     = 'Processing Queue'
  s.description = 'A multi-process work queue backed by Redis'
  s.authors     = ['Rômulo A. Ceccon']
  s.email       = 'romuloceccon@gmail.com'
  s.homepage    = 'http://www.vertitecnologia.com.br/'
  s.files       = ['lib/processing_queue.rb', 'lib/processing_queue/monitor.rb']
  s.executables = ['queue-monitor']
  s.license     = 'Nonstandard'

  s.add_runtime_dependency 'curses', ['~> 1.0']
  s.add_runtime_dependency 'redis', ['~> 3.0']

  s.add_development_dependency 'mocha', ['~> 1.0']
  s.add_development_dependency 'test-unit', ['~> 3.0']
end
