# frozen_string_literal: true
require 'sidekiq'

module Sidekiq
  module PriorityQueue
    class ReliableFetch

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
        @process_index = options[:index]
      end

      def retrieve_work
        work = @queues.detect do |q|
          job = spop(wip_queue(q))
          break [q,job] if job
          job = zpopmin_sadd(q, wip_queue(q));
          break [q,job] if job
        end
        UnitOfWork.new(*work, wip_queue(work.first)) if work
      end

      def wip_queue(q)
        "#{q}_#{Socket.gethostname}_#{@process_index}"
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

      def self.bulk_requeue(_inprogress, options)
        jobs_to_requeue = {}
        Sidekiq.redis do |conn|
          Sidekiq.logger.debug { "Re-queueing terminated jobs" }
          options[:queues].map { |q| "priority-queue:#{q}" }.each do |q|
            jobs_to_requeue[q] = []
            wip_queue = "#{q}_#{Socket.gethostname}_#{options[:index]}"
            while job = conn.spop(wip_queue) do
              jobs_to_requeue[q] << job
            end
          end

          conn.pipelined do
            jobs_to_requeue.each do |queue, jobs|
              conn.zadd(queue, jobs.map{|j| [0,j] })
            end
          end
        end
        Sidekiq.logger.info("Pushed #{ jobs_to_requeue.map{|q| q.size }.sum } jobs back to Redis")
      rescue => ex
        Sidekiq.logger.warn("Failed to requeue #{ jobs_to_requeue.map{|q| q.size }.sum } jobs: #{ex.message}")
      end
    end
  end
end
