require 'sidekiq/web'

module Sidekiq::PriorityQueue
  module Web
    ROOT = File.expand_path('../web', __FILE__)

    def self.registered(app)
      app.tabs['Priority Queues'] = 'priority_queues'
      app.get '/priority_queues' do
        @queues = Queue.all
        render(:erb, File.read("#{ROOT}/views/priority_queues.erb"))
      end

      app.get '/priority_queues/:name' do
        @name = route_params[:name]
        halt(404) unless @name

        @count = (params['count'] || 25).to_i
        @queue = Sidekiq::Queue.new(@name)
        (@current_page, @total_size, @messages) = page("priority-queue:#{@name}", params['page'], @count)
        @messages = @messages.map { |msg| Sidekiq::Job.new(msg.first, @name) }
        render(:erb, File.read("#{ROOT}/views/priority_queue.erb"))
      end
    end
  end  
end

::Sidekiq::Web.register Sidekiq::PriorityQueue::Web
