module Sidekiq
  module PriorityQueue
    class Client

      # inserted into Sidekiq's Client as middleware
      def call(worker_class, item, queue, redis_pool)
        if item['priority']
          sadd('priority-queues', queue)
          zadd(queue, item['priority'], item)
          return item['jid']
        elsif item['subqueue']
          # replace the proc with what it returns
          sadd('priority-queues', queue)
          item['subqueue'] = resolve_subqueue(item['subqueue'], item['args'])
          priority = fetch_and_add(queue, item['subqueue'], item)
          zadd(queue, priority, item)
          return item['jid']
        else
          # continue pushing the normal Sidekiq way
          yield
        end
      end

      private

      def resolve_subqueue(subqueue, job_args)
        return subqueue unless subqueue.respond_to?(:call)

        subqueue.call(job_args)
      end

      def zadd(queue, score, item)
        Sidekiq.redis do |conn|
          queue = "priority-queue:#{queue}"
          conn.zadd(queue, score, item.to_json)
          return item
        end
      end

      def sadd(set, member)
        Sidekiq.redis do |conn|
          conn.sadd(set,member)
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

Sidekiq::Client.class_eval do
  def push(item)
    normed = normalize_item(item)
    payload = process_single(item['class'], normed)

    # if payload is a JID because the middleware already pushed then just return the JID
    return payload if payload.is_a?(String)

    if payload
      raw_push([payload])
      payload['jid']
    end
  end
end
