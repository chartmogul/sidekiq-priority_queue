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

    it 'pushes to a priority queue' do
      Sidekiq.redis {|c| c.flushdb }
      assert Worker.set(priority: 1).perform_async(1)
      assert_equal 1, Sidekiq::Priority::Queue.new().size
      job_count = Sidekiq.redis {|c| c.zcount('priority-queue:default', 1, 1) }
      assert_equal 1, job_count
    end

    class PrioritizedWorker
      include Sidekiq::Worker

      def self.prioritization_key(foo,bar,foobar)
        return foo
      end
    end

    it 'prioritises based on already enqueued jobs for the same key' do
      Sidekiq.redis {|c| c.flushdb }
      assert PrioritizedWorker.perform_async(1)
      assert PrioritizedWorker.perform_async(1)
      assert_equal 2, Sidekiq::Priority::Queue.new().size
      job_count_p1 = Sidekiq.redis {|c| c.zcount('priority-queue:default', 1, 1) }
      job_count_p2 = Sidekiq.redis {|c| c.zcount('priority-queue:default', 2, 2) }
      assert_equal 1, job_count_p1
      assert_equal 1, job_count_p2
      job, _ = Sidekiq.redis {|c| c.zrevrange('priority-queue:default', 1, 1) }
      assert_equal 1, JSON.parse(job)['prioritization_key']
    end
  end
end
