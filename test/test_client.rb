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
      assert_equal 1, Sidekiq::PriorityQueue::Queue.new().size
      job_count = Sidekiq.redis {|c| c.zcount('priority-queue:default', 0, 0) }
      assert_equal 1, job_count
      assert_equal 0, Sidekiq::Queue.new().size
    end

    class PrioritizedWorker
      include Sidekiq::Worker

      sidekiq_options subqueue: ->(args){ args[0] }
    end

    it 'prioritises based on already enqueued jobs for the same key' do
      Sidekiq.redis {|c| c.flushdb }
      assert PrioritizedWorker.perform_async('user_1', 'enqueued_first')
      assert PrioritizedWorker.perform_async('user_1', 'enqueued_second')
      assert PrioritizedWorker.perform_async('user_2', 'enqueued_third')

      assert_equal 3, Sidekiq::PriorityQueue::Queue.new().size

      jobs_by_priority = Sidekiq
        .redis { |c| c.zrange('priority-queue:default', 0, 2, withscores: true) }
        .map   { |args, priority| [JSON.parse(args)['args'], priority] }

      jobs_first_priority, jobs_lower_priority = jobs_by_priority.partition { |j| j[1] == 1 }

      # NOTE: The ordering of keys with the same score is lexicographical (sorta alphabetical)
      #       https://redis.io/commands/zrange
      #       Therefore we don't test the ordering within the same-score elements.
      assert_first_jobs = [
        [["user_2", "enqueued_third"], 1.0],
        [["user_1", "enqueued_first"], 1.0]
      ]
      assert_second_jobs = [
        [["user_1", "enqueued_second"], 2.0]
      ]

      assert_equal assert_first_jobs.sort, jobs_first_priority.sort
      assert_equal assert_second_jobs.sort, jobs_lower_priority.sort
    end
  end
end
