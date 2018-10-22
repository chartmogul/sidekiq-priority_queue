# frozen_string_literal: true
require_relative 'helper'

class TestClient < Sidekiq::Test

  describe 'client' do
    class Worker
      include Sidekiq::Worker
    end

    it 'allows standard sidekiq functionality' do
      Sidekiq.redis {|c| c.flushdb }
      assert Worker.perform_async(1)
      assert_equal 1, Sidekiq::Queue.new().size
    end

    it 'pushes to a priority queue and not a normal queue' do
      Sidekiq.redis {|c| c.flushdb }
      assert Worker.set(priority: 0).perform_async(1)
      assert_equal 0, Sidekiq::Queue.new().size
      q = Sidekiq::PriorityQueue::Queue.new()
      assert_equal 1, q.size
      assert_equal 0, q.first.priority
    end

    class PrioritizedWorker
      include Sidekiq::Worker
      sidekiq_options subqueue: ->(args){ args[0] }
    end

    it 'prioritises based on already enqueued jobs for the same key' do
      Sidekiq.redis {|c| c.flushdb }
      # NOTE: The ordering of keys with the same score is lexicographical: https://redis.io/commands/zrange
      jobs_with_expected_priority = [ ['a',1], ['b',1], ['a',2] ]
      jobs_with_expected_priority.each{|arg,_| PrioritizedWorker.perform_async(arg) }

      queue = Sidekiq::PriorityQueue::Queue.new
      assert_equal 3, queue.size

      assert_equal jobs_with_expected_priority, queue.map{ |q| [q.subqueue, q.priority] }
    end
  end
end
