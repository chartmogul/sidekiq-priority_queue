module Sidekiq
  module Priority
    class Push

      #hook used by Sidekiq middleware
      def call(worker_class, item, queue, redis_pool)
        if item['prioritized_by']
          Sidekiq.redis do |conn|
            queue = "priority-queue:#{queue}"
            conn.zadd(queue, 0, item.to_json)
            return item
          end
        else
          # continue pushing the normal Sidekiq way
          yield
        end
      end
    end
  end
end
