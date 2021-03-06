# frozen_string_literal: true
require 'sidekiq'

module Sidekiq
  module PriorityQueue
    class Fetch

      UnitOfWork = Struct.new(:queue, :job) do
        def acknowledge
          Sidekiq.redis do |conn|
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
          Sidekiq.redis do |conn|
            conn.zadd(queue, 0, job)
          end
        end
      end

      def initialize(options)
        @strictly_ordered_queues = !!options[:strict]
        @queues = options[:queues].map { |q| "priority-queue:#{q}" }
        @queues = @queues.uniq if @strictly_ordered_queues
      end

      def retrieve_work
        work = @queues.detect{ |q| job = zpopmin(q); break [q,job] if job }
        UnitOfWork.new(*work) if work
      end

      def zpopmin(queue)
        Sidekiq.redis do |con|
          @script_sha ||= con.script(:load, Sidekiq::PriorityQueue::Scripts::ZPOPMIN)
          con.evalsha(@script_sha, [queue])
        end
      end

      def queues_cmd
        if @strictly_ordered_queues
          @queues
        else
          @queues.shuffle.uniq
        end
      end

      def bulk_requeue(inprogress, options)
        return if inprogress.empty?

        Sidekiq.logger.debug { "Re-queueing terminated jobs" }
        jobs_to_requeue = {}
        inprogress.each do |unit_of_work|
          jobs_to_requeue[unit_of_work.queue] ||= []
          jobs_to_requeue[unit_of_work.queue] << unit_of_work.job
        end

        Sidekiq.redis do |conn|
          conn.pipelined do
            jobs_to_requeue.each do |queue, jobs|
              conn.zadd(queue, jobs.map{|j| [0,j] })
            end
          end
        end
        Sidekiq.logger.info("Pushed #{inprogress.size} jobs back to Redis")
      rescue => ex
        Sidekiq.logger.warn("Failed to requeue #{inprogress.size} jobs: #{ex.message}")
      end
    end
  end
end
