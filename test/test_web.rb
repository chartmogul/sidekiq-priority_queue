# encoding: utf-8
# frozen_string_literal: true
require_relative 'helper'
require 'sidekiq/web'
require 'rack/test'

class TestWeb < Sidekiq::Test
  describe 'sidekiq web' do
    include Rack::Test::Methods

    def app
      Sidekiq::Web
    end

    class PrioritizedWebWorker
      include Sidekiq::Worker
      sidekiq_options subqueue: ->(args){ args[0] }

      def perform(a,b,c)
      end
    end

    job = { 'jid' => 'blah', 'class' => 'FakeWorker', 'args' => [1,2,3], 'subqueue' => 1 }
    job_2 = { 'jid' => 'blahah', 'class' => 'FakeWorkerDef', 'args' => [1,2,3,4], 'subqueue' => 1 }

    before do
      Sidekiq.redis = { :url => REDIS_URL }
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.sadd('priority-queues', 'priority-queue:default')
        conn.zadd('priority-queue:default', 0, job.to_json)
        conn.zadd("priority-queue-counts:default", 2, job_2['subqueue'])
      end
    end

    it 'can display queues' do
      get '/priority_queues'
      assert_equal 200, last_response.status
      assert_match(/default/, last_response.body)
    end

    it 'can display queue' do
      get '/priority_queues/default'
      assert_equal 200, last_response.status
      assert_match(/FakeWorker/, last_response.body)
    end

    it 'can delete job' do
      post '/priority_queues/default/delete', key_val: job.to_json
      assert_equal 302, last_response.status
      assert_equal 0, Sidekiq::PriorityQueue::Queue.new('default').size
    end

  end
end
