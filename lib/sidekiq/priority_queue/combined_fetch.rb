# frozen_string_literal: true

module Sidekiq
  module PriorityQueue
    class CombinedFetch
      def initialize(&block)
        @fetches = []
        yield self
      end

      def add(fetch)
        @fetches << fetch
      end

      def retrieve_work
        @fetches.each do |fetch|
          work = fetch.retrieve_work
          return work if work
        end
      end
    end
  end
end
