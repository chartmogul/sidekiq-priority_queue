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
        @process_index = options[:index] || ENV['PROCESS_INDEX']
      end

      def retrieve_work
        work = @queues.detect do |q|
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

      def bulk_requeue(_inprogress, options)
        Sidekiq.logger.debug { "Re-queueing terminated jobs" }
        process_index = options[:index] || ENV['PROCESS_INDEX']
        self.class.requeue_wip_jobs(options[:queues], process_index)
      end

      def self.resume_wip_jobs(queues, process_index)
        Sidekiq.logger.debug { "Re-queueing WIP jobs" }
        process_index ||= ENV['PROCESS_INDEX']
        requeue_wip_jobs(queues, process_index)
      end

      Sidekiq.configure_server do |config|
        config.on(:startup) do
          if reliable_fetch_active?(config)
            Sidekiq::PriorityQueue::ReliableFetch.resume_wip_jobs(config.options[:queues], config.options[:index])
          end
        end
      end

      private

      def self.reliable_fetch_active?(config)
        return true if config.options[:fetch].is_a?(Sidekiq::PriorityQueue::ReliableFetch)
        return config.options[:fetch].is_a?(Sidekiq::PriorityQueue::CombinedFetch) &&
          config.options[:fetch].fetches.any? { |f| f.is_a?(Sidekiq::PriorityQueue::ReliableFetch) }
      end

      def self.requeue_wip_jobs(queues, index)
        jobs_to_requeue = {}
        Sidekiq.redis do |conn|
          queues.map { |q| "priority-queue:#{q}" }.each do |q|
            wip_queue = "#{q}_#{Socket.gethostname}_#{index}"
            jobs_to_requeue[q] = []
            while job = conn.spop(wip_queue) do
              jobs_to_requeue[q] << job
            end
          end

          conn.pipelined do
            jobs_to_requeue.each do |queue, jobs|
              return unless jobs.size > 0
              conn.zadd(queue, jobs.map{|j| [0,j] })
            end
          end
        end
        Sidekiq.logger.info("Pushed #{ jobs_to_requeue.map{|q| q.size }.reduce(:+) } jobs back to Redis")
      rescue => ex
        Sidekiq.logger.warn("Failed to requeue #{ jobs_to_requeue.map{|q| q.size }.reduce(:+) } jobs: #{ex.message}")
      end
    end
  end
end
