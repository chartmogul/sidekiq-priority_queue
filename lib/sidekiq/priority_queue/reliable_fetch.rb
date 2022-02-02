# frozen_string_literal: true

require 'sidekiq'
require 'sidekiq/util'

module Sidekiq
  module PriorityQueue
    class ReliableFetch
      include Sidekiq::Util

      SUPER_PROCESSES_REGISTRY_KEY = 'super_processes_priority'

      UnitOfWork = Struct.new(:queue, :job, :wip_queue) do
        def acknowledge
          Sidekiq.redis do |conn|
            conn.srem(wip_queue, job)
            unless subqueue.nil?
              count = conn.zincrby(subqueue_counts, -1, subqueue)
              conn.zrem(subqueue_counts, subqueue) if count < 1
            end
          end
        end

        def queue_name
          queue.sub(/.*queue:/, '')
        end

        def subqueue
          @parsed_job ||= JSON.parse(job)
          @parsed_job['subqueue']
        end

        def subqueue_counts
          "priority-queue-counts:#{queue_name}"
        end

        def requeue
          # Nothing needed. Jobs are in WIP queue.
        end
      end

      def initialize(options)
        @options = options
        @strictly_ordered_queues = !!options[:strict]
        @queues = options[:queues].map { |q| "priority-queue:#{q}" }
        @queues = @queues.uniq if @strictly_ordered_queues
        @done = false
        @process_index = options[:index] || ENV['PROCESS_INDEX']
      end

      def setup
        Sidekiq.on(:startup) do
          cleanup_the_dead
          register_myself
          check
        end
        Sidekiq.on(:shutdown) do
          @done = true
        end
        Sidekiq.on(:heartbeat) do
          register_myself
        end
      end

      def retrieve_work
        return nil if @done

        work = @queues.detect do |q|
          job = zpopmin_sadd(q, wip_queue(q))
          break [q, job] if job
        end
        UnitOfWork.new(*work, wip_queue(work.first)) if work
      end

      def wip_queue(q)
        "queue:spriorityq|#{identity}|#{q}"
      end

      def zpopmin_sadd(queue, wip_queue)
        Sidekiq.redis do |conn|
          @script_sha ||= conn.script(:load, Sidekiq::PriorityQueue::Scripts::ZPOPMIN_SADD)
          conn.evalsha(@script_sha, [queue, wip_queue])
        end
      end

      def spop(wip_queue)
        Sidekiq.redis { |con| con.spop(wip_queue) }
      end

      def queues_cmd
        if @strictly_ordered_queues
          @queues
        else
          @queues.shuffle.uniq
        end
      end

      # Below method is called when we close sidekiq process gracefully
      def bulk_requeue(_inprogress, _options)
        Sidekiq.logger.debug { 'Priority ReliableFetch: Re-queueing terminated jobs' }
        requeue_wip_jobs
        unregister_super_process
      end

      private

      def check
        check_for_orphans if orphan_check?
      rescue StandardError => e
        # orphan check is best effort, we don't want Redis downtime to
        # break Sidekiq
        Sidekiq.logger.warn { "Priority ReliableFetch: Failed to do orphan check: #{e.message}" }
      end

      def orphan_check?
        delay = @options.fetch(:super_fetch_orphan_check, 3600).to_i
        return false if delay.zero?

        Sidekiq.redis do |conn|
          conn.set('priority_reliable_fetch_orphan_check', Time.now.to_f, ex: delay, nx: true)
        end
      end

      # This method is extra paranoid verification to check Redis for any possible
      # orphaned queues with jobs. If we change queue names and lose jobs in the meantime,
      # this will find old queues with jobs and rescue them.
      def check_for_orphans
        orphans_count = 0
        queues_count = 0
        orphan_queues = Set.new
        Sidekiq.redis do |conn|
          ids = conn.smembers(SUPER_PROCESSES_REGISTRY_KEY)
          Sidekiq.logger.debug("Priority ReliableFetch found #{ids.size} super processes")

          conn.scan_each(match: 'queue:spriorityq|*', count: 100) do |wip_queue|
            queues_count += 1
            _, id, original_priority_queue_name = wip_queue.split('|')
            next if ids.include?(id)

            # Race condition in pulling super_processes and checking queue liveness.
            # Need to verify in Redis.
            unless conn.sismember(SUPER_PROCESSES_REGISTRY_KEY, id)
              orphan_queues << original_priority_queue_name
              queue_jobs_count = 0
              loop do
                break if conn.scard(wip_queue).zero?

                # Here we should wrap below two operations in Lua script
                item = conn.spop(wip_queue)
                conn.zadd(original_priority_queue_name, 0, item)
                orphans_count += 1
                queue_jobs_count += 1
              end
              if queue_jobs_count.positive?
                Sidekiq::Pro.metrics.increment('jobs.recovered.fetch', by: queue_jobs_count, tags: ["queue:#{original_priority_queue_name}"])
              end
            end
          end
        end

        if orphans_count.positive?
          Sidekiq.logger.warn { "Priority ReliableFetch recovered #{orphans_count} orphaned jobs in queues: #{orphan_queues.to_a.inspect}" }
        elsif queues_count.positive?
          Sidekiq.logger.info { "Priority ReliableFetch found #{queues_count} working queues with no orphaned jobs" }
        end
        orphans_count
      end

      # Below method is only to make sure we get jobs from incorrectly closed process (for example force killed using kill -9 SIDEKIQ_PID)
      def cleanup_the_dead
        overall_moved_count = 0
        Sidekiq.redis do |conn|
          conn.sscan_each(SUPER_PROCESSES_REGISTRY_KEY) do |super_process|
            next if conn.exists?(super_process) # Don't clean up currently running processes

            Sidekiq.logger.debug { "Priority ReliableFetch: Moving job from #{super_process} back to original queues" }

            # We need to pushback any leftover jobs still in WIP
            previously_handled_queues = conn.smembers("#{super_process}:super_priority_queues")

            # Below previously_handled_queues are simply WIP queues of previous, dead processes
            previously_handled_queues.each do |previously_handled_queue|
              queue_moved_size = 0
              original_priority_queue_name = previously_handled_queue.split('|').last

              Sidekiq.logger.debug { "Priority ReliableFetch: Moving job from #{previously_handled_queue} back to original queue: #{original_priority_queue_name}" }
              loop do
                break if conn.scard(previously_handled_queue).zero?

                # Here we should wrap below two operations in Lua script
                item = conn.spop(previously_handled_queue)
                conn.zadd(original_priority_queue_name, 0, item)
                queue_moved_size += 1
                overall_moved_count += 1
              end
              # Below we simply remove old WIP queue
              conn.del(previously_handled_queue) if conn.scard(previously_handled_queue).zero?
              Sidekiq.logger.debug { "Priority ReliableFetch: Moved #{queue_moved_size} jobs from ##{previously_handled_queue} back to original_queue: #{original_priority_queue_name} " }
            end

            Sidekiq.logger.debug { "Priority ReliableFetch: Unregistering super process #{super_process}" }
            conn.del("#{super_process}:super_priority_queues")
            conn.srem(SUPER_PROCESSES_REGISTRY_KEY, super_process)
          end
        end
        Sidekiq.logger.debug { "Priority ReliableFetch: Moved overall #{overall_moved_count} jobs from WIP queues" }
      rescue StandardError => e
        # best effort, ignore Redis network errors
        Sidekiq.logger.warn { "Priority ReliableFetch: Failed to requeue: #{e.message}" }
      end

      def requeue_wip_jobs
        jobs_to_requeue = {}
        Sidekiq.redis do |conn|
          @queues.each do |q|
            wip_queue_name = wip_queue(q)
            jobs_to_requeue[q] = []

            while job = conn.spop(wip_queue_name)
              jobs_to_requeue[q] << job
            end
          end

          conn.pipelined do
            jobs_to_requeue.each do |queue, jobs|
              next if jobs.empty? # ZADD doesn't work with empty arrays

              conn.zadd(queue, jobs.map { |j| [0, j] })
            end
          end
        end
        Sidekiq.logger.info("Priority ReliableFetch: Pushed #{jobs_to_requeue.values.flatten.size} jobs back to Redis")
      rescue StandardError => e
        Sidekiq.logger.warn("Priority ReliableFetch: Failed to requeue #{jobs_to_requeue.values.flatten.size} jobs: #{e.message}")
      end

      def register_myself
        super_process_wip_queues = @queues.map { |q| wip_queue(q) }
        id = identity # This is from standard sidekiq, updated with every heartbeat

        # This method will run multiple times so seeing this message twice is no problem.
        Sidekiq.logger.debug { "Priority ReliableFetch: Registering super process #{id} with #{super_process_wip_queues}" }

        Sidekiq.redis do |conn|
          conn.multi do
            conn.sadd(SUPER_PROCESSES_REGISTRY_KEY, id)
            conn.sadd("#{id}:super_priority_queues", super_process_wip_queues)
          end
        end
      end

      def unregister_super_process
        id = identity
        Sidekiq.logger.debug { "Priority ReliableFetch: Unregistering super process #{id}" }
        Sidekiq.redis do |conn|
          conn.multi do
            conn.srem(SUPER_PROCESSES_REGISTRY_KEY, id)
            conn.del("#{id}:super_priority_queues")
          end
        end
      end
    end
  end
end
