# frozen_string_literal: true
require_relative 'helper'
require 'sidekiq/fetch'

class TestFetcher < Sidekiq::Test
  describe 'fetcher' do
    normal_job = {'jid' => 'normal_job', 'args' => [1,2,3] }
    priority_job = {'jid' => 'priority_job', 'args' => [1,2,3], 'subqueue' => 1 }

    before do
      Sidekiq.redis = { :url => REDIS_URL }
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.lpush('queue:foo', normal_job.to_json)
        conn.zadd('priority-queue:foo', 0, priority_job.to_json)
        conn.zadd("priority-queue-counts:foo", 1, priority_job['subqueue'])
      end
    end

    after do
      Sidekiq.redis = REDIS
    end

    it 'retrieves from both normal and priority queues' do
      fetch = Sidekiq::PriorityQueue::CombinedFetch.configure do |fetches|
        fetches.add Sidekiq::BasicFetch.new(queues: ['foo'])
        fetches.add Sidekiq::PriorityQueue::Fetch.new(queues: ['foo'])
      end

      uow = fetch.retrieve_work
      refute_nil uow
      assert_equal 'foo', uow.queue_name
      assert_equal normal_job.to_json, uow.job

      uow = fetch.retrieve_work
      refute_nil uow
      assert_equal 'foo', uow.queue_name
      assert_equal priority_job.to_json, uow.job
    end

    it 'bulk requeues all jobs only once' do
      fetch = Sidekiq::PriorityQueue::CombinedFetch.configure do |fetches|
        fetches.add Sidekiq::BasicFetch.new(queues: ['foo'], index: 0)
        fetches.add Sidekiq::PriorityQueue::Fetch.new(queues: ['foo'], index: 0)
      end

      q1 = Sidekiq::PriorityQueue::Queue.new('foo')
      q2 = Sidekiq::Queue.new('foo')
      assert_equal 1, q1.size
      assert_equal 1, q2.size
      uow = Sidekiq::PriorityQueue::Fetch::UnitOfWork

      Sidekiq.redis do |conn|
        conn.sadd("priority-queue:foo_#{Socket.gethostname}_0", 'bob')
      end

      fetch.bulk_requeue(
        [ uow.new('priority-queue:foo', 'bob'), uow.new('queue:foo', 'bar') ],
        { queues: ['foo'], index: 0 }
      )

      assert_equal 2, q1.size
      assert_equal 2, q2.size
    end
  end
end
