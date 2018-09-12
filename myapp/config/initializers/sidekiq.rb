require 'sidekiq/fetch'

Sidekiq.configure_server do |config|
  config.options[:fetch] = Sidekiq::PriorityQueue::CombinedFetch.configure do |fetches|
    fetches.add Sidekiq::PriorityQueue::ReliableFetch
    fetches.add Sidekiq::BasicFetch
  end
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::PriorityQueue::Client
  end
end
