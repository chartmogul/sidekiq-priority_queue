# frozen_string_literal: true
require 'sidekiq'

module Sidekiq
  module Priority
    class ReliableFetch

      UnitOfWork = Struct.new(:queue, :job) do
        def acknowledge
          Sidekiq.redis do |conn|
            parsed_job = JSON.parse(job)
            conn.srem("#{queue}_#{Socket.gethostname}", job)
            unless parsed_job['subqueue'].nil?
              count = conn.zincrby("priority-queue-counts:#{queue_name}", -1, parsed_job['subqueue'])
              conn.zrem("priority-queue-counts:#{queue_name}", parsed_job['subqueue']) if count < 1
            end
          end
        end

        def queue_name
          queue.sub(/.*queue:/, '')
        end

        def requeue
          # Nothing needed. Jobs are in private queue.
        end
      end

      def initialize(options)
        @strictly_ordered_queues = !!options[:strict]
        @queues = options[:queues].map { |q| "priority-queue:#{q}" }
        @queues = @queues.uniq if @strictly_ordered_queues
      end

      def retrieve_work
        work = @queues.detect do |q|
          job = spop(wip_queue_name(q))
          break [q,job] if job
          job = zpopmin_sadd(q, wip_queue_name(q));
          break [q,job] if job
        end
        UnitOfWork.new(*work) if work
      end

      def wip_queue_name(q)
        "#{q}_#{Socket.gethostname}"
      end

      def zpopmin_sadd(queue, wip_queue)
        Sidekiq.redis do |con|
          @script_sha ||= con.script(:load, Sidekiq::Priority::Scripts::ZPOPMIN_SADD)
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

      def self.bulk_requeue(inprogress, options)
        return if inprogress.empty?

        Sidekiq.logger.debug { "Re-queueing terminated jobs" }
        jobs_to_requeue = {}
        inprogress.each do |unit_of_work|
          jobs_to_requeue[unit_of_work.queue_name] ||= []
          jobs_to_requeue[unit_of_work.queue_name] << unit_of_work.job
        end

        Sidekiq.redis do |conn|
          conn.pipelined do
            jobs_to_requeue.each do |queue, jobs|
              conn.zadd("priority-queue:#{queue}", jobs.map{|j| [0,j] })
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
