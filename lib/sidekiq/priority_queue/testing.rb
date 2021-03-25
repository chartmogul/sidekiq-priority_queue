# frozen_string_literal: true

# Don't require in production code.
# This disables the middleware and falls back to normal push, meaning in tests it will use inline/fake mode.
# Prioritization doesn't make any sense in inline or fake tests anyway.
module Sidekiq
  module PriorityQueue
    module TestingClient
      def call(worker_class, item, queue, redis_pool)
        testing_verify_subqueue(item) if item['subqueue'] && !item['priority']
        yield # continue pushing the normal Sidekiq way
      end

      # Help testing the lambda; raise in case it's invalid.
      def testing_verify_subqueue(item)
        subqueue = resolve_subqueue(item['subqueue'], item['args'])
        serialized = "#{subqueue}"

        raise "subqueue shouldn't be nil" if subqueue.nil?
        raise "subqueue shouldn't be empty" if serialized == ""
      end
    end

    Sidekiq::PriorityQueue::Client.prepend TestingClient
  end
end