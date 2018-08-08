# frozen_string_literal: true

module Sidekiq
  module PriorityQueue
    class Queue
      def initialize(name="default")
        @name = name
        @rname = "priority-queue:#{name}"
      end

      def size
        Sidekiq.redis { |con| con.zcard(@rname) }
      end
    end
  end
end
