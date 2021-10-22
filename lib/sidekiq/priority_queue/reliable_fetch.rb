# frozen_string_literal: true
require 'sidekiq'
require 'sidekiq/util'

module Sidekiq
  module PriorityQueue
    class ReliableFetch
      include Sidekiq::Util

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
          job = zpopmin_sadd(q, wip_queue(q));
          break [q,job] if job
        end
        UnitOfWork.new(*work, wip_queue(work.first)) if work
      end

      def wip_queue(q)
        "queue:spriorityq|#{identity}|#{q}"
      end

      def zpopmin_sadd(queue, wip_queue)
        Sidekiq.redis do |con|
          @script_sha ||= con.script(:load, Sidekiq::PriorityQueue::Scripts::ZPOPMIN_SADD)
          con.evalsha(@script_sha, [queue, wip_queue])
        end
      end

      def spop(wip_queue)
        Sidekiq.redis{ |con| con.spop(wip_queue) }
      end

      def queues_cmd
        if @strictly_ordered_queues
          @queues
        else
          @queues.shuffle.uniq
        end
      end

      def bulk_requeue(_inprogress, _options)
        Sidekiq.logger.debug { "Priority ReliableFetch: Re-queueing terminated jobs" }
        requeue_wip_jobs
        unregister_super_process
      end

      private

      def cleanup_the_dead
        overall_moved_count = 0
        Sidekiq.redis do |conn|
          conn.sscan_each("super_processes_priority") do |x|
            next if conn.exists?(x) # Don't cleanup currently running processes

            Sidekiq.logger.debug { "Priority ReliableFetch: Moving job from #{x} back to original queues" }

            # We need to pushback any leftover jobs still in WIP
            previously_handled_queues = conn.smembers("#{x}:super_priority_queues")

            # Below previously_handled_queues are simply WIP queues of previous, dead processes
            previously_handled_queues.each do |previously_handled_queue|
              queue_moved_size = 0
              original_priority_queue_name = previously_handled_queue.split('|').last

              Sidekiq.logger.debug { "Priority ReliableFetch: Moving job from #{previously_handled_queue} back to original queue: #{original_priority_queue_name}" }
              loop do
                break if conn.scard(previously_handled_queue) == 0

                item = conn.spop(previously_handled_queue)
                conn.zadd(original_priority_queue_name, 0, item)
                queue_moved_size += 1
                overall_moved_count += 1
              end
              Sidekiq.logger.debug { "Priority ReliableFetch: Moved #{queue_moved_size} jobs from ##{previously_handled_queue} back to original_queue: #{original_priority_queue_name} "}
            end

            Sidekiq.logger.debug { "Priority ReliableFetch: Unregistering super process #{x}" }
            conn.del("#{x}:super_priority_queues")
            conn.srem("super_processes_priority", x)
          end
        end
        Sidekiq.logger.debug { "Priority ReliableFetch: Moved overall #{overall_moved_count} jobs from WIP queues" }
      rescue => ex
        # best effort, ignore Redis network errors
        Sidekiq.logger.warn { "Priority ReliableFetch: Failed to requeue: #{ex.message}" }
      end

      def requeue_wip_jobs
        jobs_to_requeue = {}
        Sidekiq.redis do |conn|
          @queues.each do |q|
            wip_queue_name = wip_queue(q)
            jobs_to_requeue[q] = []

            while job = conn.spop(wip_queue_name) do
              jobs_to_requeue[q] << job
            end
          end

          conn.pipelined do
            jobs_to_requeue.each do |queue, jobs|
              next if jobs.size == 0 # ZADD doesn't work with empty arrays

              conn.zadd(queue, jobs.map {|j| [0, j] })
            end
          end
        end
        Sidekiq.logger.info("Priority ReliableFetch: Pushed #{ jobs_to_requeue.values.flatten.size } jobs back to Redis")
      rescue => ex
        Sidekiq.logger.warn("Priority ReliableFetch: Failed to requeue #{ jobs_to_requeue.values.flatten.size } jobs: #{ex.message}")
      end

      def register_myself
        qs = @queues.map { |q| wip_queue(q) }
        id = identity # This is from standard sidekiq, updated with every heartbeat

        # This method will run multiple times so seeing this message twice is no problem.
        Sidekiq.logger.debug { "Priority ReliableFetch: Registering super process #{id} with #{qs}" }

        Sidekiq.redis do |conn|
          conn.multi do
            conn.sadd("super_processes_priority", id)
            conn.sadd("#{id}:super_priority_queues", qs)
          end
        end
      end

      def unregister_super_process
        id = identity
        Sidekiq.logger.debug { "Priority ReliableFetch: Unregistering super process #{id}" }
        Sidekiq.redis do |conn|
          conn.multi do
            conn.srem("super_processes_priority", id)
            conn.del("#{id}:super_priority_queues")
          end
        end
      end
    end
  end
end
