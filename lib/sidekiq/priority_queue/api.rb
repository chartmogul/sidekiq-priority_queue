# frozen_string_literal: true
require 'sidekiq/api'


module Sidekiq
  module PriorityQueue
    class Queue
      include  Enumerable

      attr_reader :name

      def initialize(name='default')
        @name = name
        @rname = "priority-queue:#{name}"
      end

      def size
        Sidekiq.redis { |con| con.zcard(@rname) }
      end

      def each
        initial_size = size
        deleted_size = 0
        page = 0
        page_size = 50

        while true do
          range_start = page * page_size - deleted_size
          range_end   = range_start + page_size - 1
          entries = Sidekiq.redis do |conn|
            conn.zrange @rname, range_start, range_end, withscores: true
          end
          break if entries.empty?
          page += 1
          entries.each do |entry, priority|
            yield Job.new(entry, @name, priority)
          end
          deleted_size = initial_size - size
        end
      end

      def self.all
         Sidekiq.redis { |con| con.smembers('priority-queues') }
          .map{ |key| key.gsub('priority-queue:', '') }
          .sort
          .map { |q| Queue.new(q) }
      end

    end

    class Job < Sidekiq::Job

      attr_reader :priority
      attr_reader :subqueue

      def initialize(item, queue_name = nil, priority = nil)
        @args = nil
        @value = item
        @item = item.is_a?(Hash) ? item : parse(item)
        @queue = queue_name || @item['queue']
        @subqueue = @item['subqueue']
        @priority = priority
      end

      def delete
        count = Sidekiq.redis do |conn|
          conn.zrem("priority-queue:#{@queue}", @value)
        end
        count != 0
      end
    end
  end
end
