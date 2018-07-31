# frozen_string_literal: true

module Sidekiq
  module Priority
    class Queue
      def initialize(name="default")
        @name = name
        @rname = "priority-queue:#{name}"

        def size
          Sidekiq.redis { |con| con.zcard(@rname) }
        end
      end
    end
  end
end
