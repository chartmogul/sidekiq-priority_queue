require 'sidekiq/web'

module Sidekiq::PriorityQueue
  module Web
    ROOT = File.expand_path('../web', __FILE__)

    def self.registered(app)
      app.tabs['Priority Queues'] = 'priority_queues'
      app.get '/priority_queues' do
        @queues = []
        render(:erb, File.read("#{ROOT}/views/priority_queues.erb"))
      end
    end
  end  
end

::Sidekiq::Web.register Sidekiq::PriorityQueue::Web
