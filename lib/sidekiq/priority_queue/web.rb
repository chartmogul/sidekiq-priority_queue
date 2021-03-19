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
        @subqueue_counts = Sidekiq.redis do |con|
          con.zrevrange("priority-queue-counts:#{@name}", 0, params['subqueue_count'] || 10, withscores: true)
        end.map { |name, count| SubqueueCount.new(name, count) }

        @messages = @messages.map{ |msg| Job.new(msg.first, @name, msg.last) }
        render(:erb, File.read("#{ROOT}/views/priority_queue.erb"))
      end

      app.post "/priority_queues/:name/delete" do
        name = route_params[:name]
        Job.new(params['key_val'], name).delete
        redirect_with_query("#{root_path}priority_queues/#{CGI.escape(name)}")
      end

    end
  end
end

::Sidekiq::Web.register Sidekiq::PriorityQueue::Web
