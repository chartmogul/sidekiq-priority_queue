# frozen_string_literal: true
require_relative 'helper'

class TestApi < Sidekiq::Test
  describe 'Queue' do

    before do
      Sidekiq.redis = { :url => REDIS_URL }
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.zadd('priority-queue:foo', 0, 'blah')
        conn.zadd("priority-queue-counts:foo", 1, 'blah')
      end
    end

    it 'works' do
      assert_equal 1, Sidekiq::PriorityQueue::Queue.all.size
    end

  end
end
