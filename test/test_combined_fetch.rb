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
      fetch = Sidekiq::PriorityQueue::CombinedFetch.new do |fetches|
        fetches.add Sidekiq::BasicFetch.new(:queues => ['foo'])
        fetches.add Sidekiq::PriorityQueue::Fetch.new(:queues => ['foo'])
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
  end
end
