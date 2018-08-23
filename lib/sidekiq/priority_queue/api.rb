# frozen_string_literal: true
require 'sidekiq/api'


module Sidekiq
  module PriorityQueue
    class Queue

      attr_reader :name

      def initialize(name='default')
        @name = name
        @rname = "priority-queue:#{name}"
      end

      def size
        Sidekiq.redis { |con| con.zcard(@rname) }
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
