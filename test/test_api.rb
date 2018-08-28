# frozen_string_literal: true
require_relative 'helper'

class TestApi < Sidekiq::Test
  describe 'Queue' do

    before do
      Sidekiq.redis = { :url => REDIS_URL }
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.sadd('priority-queues', 'priority-queue:foo')
        conn.zadd('priority-queue:foo', 0, 'blah')
        conn.zadd("priority-queue-counts:foo", 1, 'blah')
      end
    end

    it 'works' do
      assert_equal 1, Sidekiq::PriorityQueue::Queue.all.size
      assert_equal 1, Sidekiq::PriorityQueue::Queue.all.first.size
    end

    it 'can enumerate jobs' do
      assert_equal ["blah"], Sidekiq::PriorityQueue::Queue.new('foo').first.args
    end

  end
end
