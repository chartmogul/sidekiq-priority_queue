module Sidekiq
  module Priority
    class Client

      #inserted into Sidekiq's Client as middleware
      def call(worker_class, item, queue, redis_pool)
        if item['priority']
          zadd(queue, item['priority'], item)
        elsif item['subqueue']
          # replace the proc with what it returns
          item['subqueue'] = item['subqueue'].call(item['args'])
          priority = fetch_and_add(queue, item['subqueue'], item)
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

      def fetch_and_add(queue, subqueue, item)
        Sidekiq.redis do |conn|
          priority = conn.zincrby("priority-queue-counts:#{queue}", 1, subqueue)
        end
      end
    end
  end
end
