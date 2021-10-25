# frozen_string_literal: true
require_relative 'helper'

class TestFetcher < Sidekiq::Test

  describe 'reliable fetcher' do
    job = {'jid' => 'blah', 'args' => [1,2,3], 'subqueue' => 1 }

    before do
      Sidekiq.redis = { :url => REDIS_URL }
      Sidekiq.redis do |conn|
        conn.flushdb
        conn.zadd('priority-queue:foo', 0, job.to_json)
        conn.zadd("priority-queue-counts:foo", 1, job['subqueue'])
      end
      reset_sidekiq_lifecycle_events
    end

    after do
      Sidekiq.redis = REDIS
    end

    it 'adds three new event triggers when #setup is called' do
      assert_equal 0, Sidekiq.options[:lifecycle_events].values.flatten.size
      fetch = Sidekiq::PriorityQueue::ReliableFetch.new(queues: ['foo'], index: 0)
      fetch.setup
      assert_equal 3, Sidekiq.options[:lifecycle_events].values.flatten.size
    end

    it 'stops picking up work once shutdown event is triggered' do
      fetch = Sidekiq::PriorityQueue::ReliableFetch.new(queues: ['foo'], index: 0)
      fetch.setup

      SidekiqUtilInstance.new.fire_event(:shutdown)

      assert_equal 1, Sidekiq::PriorityQueue::Queue.new('foo').size
      assert_nil fetch.retrieve_work
    end

    it 'cleans up dead jobs on startup' do
      # First we have to mimic old jobs lying around
      previous_process_identity = "sidekiq-pipeline-32152-xfwvw:42251:#{SecureRandom.hex(6)}"
      priority_queue = "priority-queue:bar"

      Sidekiq.redis do |conn|
        conn.sadd("super_processes_priority", previous_process_identity)
        previous_wip_queue = "queue:spriorityq|#{previous_process_identity}|#{priority_queue}"
        conn.sadd("#{previous_process_identity}:super_priority_queues", previous_wip_queue)
        conn.sadd(previous_wip_queue, job.to_json)
      end

      # Then we setup the fetcher
      fetch = Sidekiq::PriorityQueue::ReliableFetch.new(queues: ['bar'], index: 0)
      fetch.setup

      # And here we test is really on startup it brings back old jobs
      assert_equal 0, Sidekiq::PriorityQueue::Queue.new('bar').size
      SidekiqUtilInstance.new.fire_event(:startup)
      assert_equal 1, Sidekiq::PriorityQueue::Queue.new('bar').size
    end

    it 'registers process and private queues on startup' do
      fetch = Sidekiq::PriorityQueue::ReliableFetch.new(queues: ['foo'], index: 0)
      fetch.setup

      SidekiqUtilInstance.new.fire_event(:startup)

      Sidekiq.redis do |conn|
        registered_processes = conn.smembers("super_processes_priority")
        assert_equal 1, registered_processes.size
        assert_equal fetch.identity, registered_processes.first
        private_queues = conn.smembers("#{registered_processes.first}:super_priority_queues")
        assert_equal 1, private_queues.size
        identity = registered_processes.first
        assert_equal "queue:spriorityq|#{identity}|priority-queue:foo", private_queues.first
      end
    end

    it 'registers process and private queues on heartbeat' do
      fetch = Sidekiq::PriorityQueue::ReliableFetch.new(queues: ['foo'], index: 0)
      fetch.setup

      SidekiqUtilInstance.new.fire_event(:heartbeat)

      Sidekiq.redis do |conn|
        registered_processes = conn.smembers("super_processes_priority")
        assert_equal 1, registered_processes.size
        assert_equal fetch.identity, registered_processes.first
        private_queues = conn.smembers("#{registered_processes.first}:super_priority_queues")
        assert_equal 1, private_queues.size
        identity = registered_processes.first
        assert_equal "queue:spriorityq|#{identity}|priority-queue:foo", private_queues.first
      end
    end

    it 'retrieves and puts into private set' do
      fetch = setup_reliable_fetcher
      uow = fetch.retrieve_work
      refute_nil uow
      assert_equal 'foo', uow.queue_name
      assert_equal job.to_json, uow.job
      identity = fetch.identity
      wip_queue = "queue:spriorityq|#{identity}|priority-queue:foo"
      Sidekiq.redis { |conn| assert conn.sismember(wip_queue, job.to_json) }
      q = Sidekiq::PriorityQueue::Queue.new('foo')
      assert_equal 0, q.size
      assert uow.acknowledge
      Sidekiq.redis do |conn|
        assert_nil conn.zscore("priority-queue-counts:foo", job['subqueue'])
        assert !conn.sismember(wip_queue, job.to_json)
      end
    end

    it 'pushes WIP jobs back to the head of the sorted set' do
      assert_equal 1, Sidekiq::PriorityQueue::Queue.new('foo').size
      fetch = setup_reliable_fetcher
      identity = fetch.identity
      wip_queue = "queue:spriorityq|#{identity}|priority-queue:foo"

      killed_job = {'jid' => 'blah_blah', 'args' => [1,2,3], 'subqueue' => 1 }
      Sidekiq.redis { |conn| conn.sadd(wip_queue, killed_job.to_json) }

      fetch.bulk_requeue(nil, nil)
      assert_equal 2, Sidekiq::PriorityQueue::Queue.new('foo').size
    end

    it 'retrieves with strict setting' do
      fetch = Sidekiq::PriorityQueue::ReliableFetch.new(:queues => ['basic', 'bar', 'bar'], :strict => true)
      cmd = fetch.queues_cmd
      assert_equal cmd, ['priority-queue:basic', 'priority-queue:bar']
    end
  end
end
