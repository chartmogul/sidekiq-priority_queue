# frozen_string_literal: true

module Sidekiq
  module PriorityQueue
    class CombinedFetch
      attr_reader :fetches

      def initialize(fetches = [])
        @fetches = fetches
      end

      def retrieve_work
        fetches.each do |fetch|
          work = fetch.retrieve_work
          return work if work
        end
      end

      def self.configure(&block)
        combined_fetch = self.new
        yield combined_fetch

        combined_fetch
      end

      def add(fetch)
        fetches << fetch
      end

      def bulk_requeue(inprogress, options)
        # ReliableFetch#bulk_equeue ignores inprogress, so it's safe to call both
        fetches.each do |f|
          if [Fetch, ReliableFetch].any? { |klass| f.is_a?(klass) }
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
