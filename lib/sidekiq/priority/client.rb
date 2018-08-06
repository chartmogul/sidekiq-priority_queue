module Sidekiq
  module Priority
    class Client

      #inserted into Sidekiq's Client as middleware
      def call(worker_class, item, queue, redis_pool)
        if item['priority']
          zadd(queue, item['priority'], item)
        elsif item['prioritized_by']
          prioritization_key = item['prioritized_by'].call(item['args'])
          priority = fetch_and_add(queue, prioritization_key, item)
          zadd(queue, priority, item)
        else
          # continue pushing the normal Sidekiq way
          yield
        end
      end

      private

      def zadd(queue, score, item)
        Sidekiq.redis do |conn|
          queue = "priority-queue:#{queue}"
          conn.zadd(queue, score, item.to_json)
          return item
        end
      end

      def fetch_and_add(queue, prioritization_key, item)
        Sidekiq.redis do |conn|
          priority = conn.zincrby("priority-queue-counts:#{queue}", 1, prioritization_key)
        end
      end
    end
  end
end
