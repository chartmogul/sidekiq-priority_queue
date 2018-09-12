# frozen_string_literal: true

module Sidekiq
  module PriorityQueue
    class CombinedFetch

      def initialize(options)
        @fetches = @@fetches.map{ |f| f.new(options) }
      end

      def retrieve_work
        @fetches.each do |fetch|
          work = fetch.retrieve_work
          return work if work
        end
      end

      def self.configure(&block)
        @@fetches = []
        yield self
        self
      end

      def self.add(fetch)
        @@fetches << fetch
      end

      def self.fetches
        @@fetches
      end

      def self.bulk_requeue(inprogress, options)
        # ReliableFetch#bulk_equeue ignores inprogress, so it's safe to call both
        @@fetches.each do |f|
          if [Fetch, ReliableFetch].include?(f)
            jobs_to_requeue = inprogress.select{|uow| uow.queue.start_with?('priority-queue:') }
            f.bulk_requeue(jobs_to_requeue, options)
          else
            jobs_to_requeue = inprogress.reject{|uow| uow.queue.start_with?('priority-queue:') }
            f.bulk_requeue(jobs_to_requeue, options)
          end
        end
      end
    end
  end
end
