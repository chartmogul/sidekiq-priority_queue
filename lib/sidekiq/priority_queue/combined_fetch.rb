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
    end
  end
end
