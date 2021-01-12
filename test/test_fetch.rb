# frozen_string_literal: true
require_relative 'helper'

class TestFetcher < Sidekiq::Test
  describe 'fetcher' do
    job = {'jid' => 'blah', 'args' => [1,2,3], 'subqueue' => 1 }

    before do
      Sidekiq.redis = { :url => REDIS_URL }
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.zadd('priority-queue:foo', 0, job.to_json)
        conn.zadd("priority-queue-counts:foo", 1, job['subqueue'])
      end
    end

    after do
      Sidekiq.redis = REDIS
    end

    it 'retrieves' do
      fetch = Sidekiq::PriorityQueue::Fetch.new(:queues => ['foo'])
      uow = fetch.retrieve_work
      refute_nil uow
      assert_equal 'foo', uow.queue_name
      assert_equal job.to_json, uow.job
      q = Sidekiq::PriorityQueue::Queue.new('foo')
      assert_equal 0, q.size
      uow.requeue
      assert_equal 1, q.size
      assert uow.acknowledge
      Sidekiq.redis do |conn|
        assert_nil conn.zscore("priority-queue-counts:foo", job['subqueue'])
      end
    end

    it 'retrieves with strict setting' do
      fetch = Sidekiq::PriorityQueue::Fetch.new(:queues => ['basic', 'bar', 'bar'], :strict => true)
      cmd = fetch.queues_cmd
      assert_equal cmd, ['priority-queue:basic', 'priority-queue:bar']
    end

    it 'bulk requeues only priority-queue jobs' do
      q1 = Sidekiq::PriorityQueue::Queue.new('foo')
      q2 = Sidekiq::PriorityQueue::Queue.new('bar')
      assert_equal 1, q1.size
      assert_equal 0, q2.size
      uow = Sidekiq::PriorityQueue::Fetch::UnitOfWork
      Sidekiq::PriorityQueue::Fetch.new(queues: []).bulk_requeue(
        [ uow.new('priority-queue:foo', 'bob'), uow.new('fuzzy:queue:foo', 'bar') ],
        queues: []
      )
      assert_equal 2, q1.size
      assert_equal 0, q2.size
    end

  end
end
