# frozen_string_literal: true
require 'sidekiq/api'


module Sidekiq
  module PriorityQueue

    module RedisScanner
      def redis_scan(conn, pattern)
        cursor = '0'
        result = []
        loop do
          cursor, values = conn.scan(cursor, match: pattern)
          result.push(*values)
          break if cursor == '0'
        end
        result
      end
    end

    class Queue
      extend PriorityQueue::RedisScanner

      attr_reader :name

      def initialize(name="default")
        @name = name
        @rname = "priority-queue:#{name}"
      end

      def size
        Sidekiq.redis { |con| con.zcard(@rname) }
      end

      def self.all
         Sidekiq.redis { |con| redis_scan(con, 'priority-queue:*') }
          .map{ |key| key.gsub('priority-queue:', '') }
          .sort
          .map { |q| Queue.new(q) }
      end
    end

    class Job < Sidekiq::Job
      def delete
        count = Sidekiq.redis do |conn|
          conn.zrem("priority-queue:#{@queue}", @value)
        end
        count != 0
      end
    end
  end
end
