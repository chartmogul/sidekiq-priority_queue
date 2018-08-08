Gem::Specification.new do |s|
  s.name        = 'sidekiq-priority_queue'
  s.version     = '0.0.0'
  s.date        = '2018-07-31'
  s.summary     = "Priority Queuing for Sidekiq"
  s.description = "An extension for Sidekiq allowing jobs in a single queue to be execued by a priority score rather than FIFO"
  s.authors     = ["Jacob Matthews"]
  s.email       = 'jake@chartmogul.com'
  s.files       = ["lib/sidekiq/priority.rb"]
  s.require_paths = ['lib/sidekiq']
  s.homepage    =
    'https://github.com/chartmogul/sidekiq-priority_queue'
  s.license       = 'MIT'

  s.add_dependency 'sidekiq', '>= 4'
  s.add_development_dependency 'minitest', '~> 5.10', '>= 5.10.1'
end