Gem::Specification.new do |s|
  s.name        = 'sidekiq-priority_queue'
  s.version     = '1.0.6'
  s.date        = '2018-07-31'
  s.summary     = "Priority Queuing for Sidekiq"
  s.description = "An extension for Sidekiq allowing jobs in a single queue to be executed by a priority score rather than FIFO"
  s.authors     = ["Jacob Matthews", "Petr Kopac"]
  s.email       = 'petr@chartmogul.com'
  s.files       = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features|pkg)/}) }
  s.homepage    = 'https://github.com/chartmogul/sidekiq-priority_queue'
  s.license     = 'MIT'
  s.required_ruby_version = '>= 2.5.0'

  s.add_dependency 'sidekiq', '>= 6.2.2'
  s.add_development_dependency 'minitest', '~> 5.10', '>= 5.10.1'
end
