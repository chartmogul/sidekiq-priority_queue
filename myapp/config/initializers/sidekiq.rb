Sidekiq.configure_server do |config|
  config.options[:fetch] = Sidekiq::PriorityQueue::ReliableFetch
end

Sidekiq.configure_client do |config|
  config.client_middleware do |chain|
    chain.add Sidekiq::PriorityQueue::Client
  end
end
