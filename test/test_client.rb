# frozen_string_literal: true
require_relative 'helper'

class TestClient < Sidekiq::Test

  describe 'client' do
    class Worker
      include Sidekiq::Worker
    end

    it 'enqueues normally' do
      Sidekiq.redis {|c| c.flushdb }
      assert Worker.perform_async(1)
      assert_equal 1, Sidekiq::Queue.new().size
    end

    it 'enqueues with a given priority' do
      Sidekiq.redis {|c| c.flushdb }
      assert Worker.set(priority: 1).perform_async(1)
      assert_equal 1, Sidekiq::Priority::Queue.new().size
      job_count = Sidekiq.redis {|c| c.zcount('priority-queue:default', 1, 1) }
      assert_equal 1, job_count
    end
  end
end
