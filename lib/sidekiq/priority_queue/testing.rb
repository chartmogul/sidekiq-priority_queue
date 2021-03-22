# frozen_string_literal: true

# Don't require in production code.
# This disables the middleware and falls back to normal push, meaning in tests it will use inline/fake mode.
# Prioritization doesn't make any sense in inline or fake tests anyway.
module Sidekiq
  module PriorityQueue
    module TestingClient
      def call(worker_class, item, queue, redis_pool)
        # call subqueue as if we used it
        resolve_subqueue(item['subqueue'], item['args']) if item['subqueue'] && !item['priority']
        yield # continue pushing the normal Sidekiq way
      end
    end

    Sidekiq::PriorityQueue::Client.prepend TestingClient
  end
end