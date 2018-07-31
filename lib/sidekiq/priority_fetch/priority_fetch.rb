# frozen_string_literal: true
require 'sidekiq'

module Sidekiq
  module PriorityFetch

    ##
    # Provides priority queue processing via a Redis script emulating ZPOPMIN
    #

    class PriorityFetch
      #TODO what meaning does timeout have when using lua scripts? none?
      TIMEOUT = 2

      #TODO should we include the priority here for bulk_requeue? probably...
      UnitOfWork = Struct.new(:queue, :job) do
        def acknowledge
          # nothing to do
        end

        def queue_name
          queue.sub(/.*queue:/, '')
        end

        def requeue
          Sidekiq.redis do |conn|
            conn.zadd("priority-queue:#{queue_name}", 0, job)
          end
        end
      end

      def initialize(options)
        @strictly_ordered_queues = !!options[:strict]
        @queues = options[:queues].map { |q| "priority-queue:#{q}" }
        if @strictly_ordered_queues
          @queues = @queues.uniq
          @queues << TIMEOUT
        end
      end

      def retrieve_work
        work = @queues.detect{ |q| job = zpopmin(q); break [q,job] if job }
        UnitOfWork.new(*work) if work
      end

      def zpopmin(queue)
        Sidekiq.redis do |con|
          @script_sha ||= con.script(:load, Sidekiq::PriorityFetch::Scripts::ZPOPMIN)
          con.evalsha(@script_sha, [queue])
        end
      end

      # Creating the Redis#brpop command takes into account any
      # configured queue weights. By default Redis#brpop returns
      # data from the first queue that has pending elements. We
      # recreate the queue command each time we invoke Redis#brpop
      # to honor weights and avoid queue starvation.
      def queues_cmd
        if @strictly_ordered_queues
          @queues
        else
          queues = @queues.shuffle.uniq
          queues << TIMEOUT
          queues
        end
      end


      # By leaving this as a class method, it can be pluggable and used by the Manager actor. Making it
      # an instance method will make it async to the Fetcher actor
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
              conn.rpush("queue:#{queue}", jobs)
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
