# frozen_string_literal: true
require_relative 'helper'

class TestClient < Sidekiq::Test

  describe 'client' do
    class PrioritizedWorker
      include Sidekiq::Worker
      sidekiq_options :prioritized_by => :account_id

    end

    it 'enqueues' do
      Sidekiq.redis {|c| c.flushdb }
      assert PrioritizedWorker.perform_async('account_id' => 1)
      assert_equal 1, Sidekiq::Priority::Queue.new().size
    end
  end
end
