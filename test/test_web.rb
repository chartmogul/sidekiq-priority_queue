# encoding: utf-8
# frozen_string_literal: true
require_relative 'helper'
require 'sidekiq/web'
require 'rack/test'

class TestWeb < Sidekiq::Test
  describe 'sidekiq web' do
    include Rack::Test::Methods

    def app
      Sidekiq::Web
    end

    class PrioritizedWebWorker
      include Sidekiq::Worker
      sidekiq_options subqueue: ->(args){ args[0] }

      def perform(a,b,c)
      end
    end

    it 'can display queues' do
      PrioritizedWebWorker.perform_async(1,2,3)

      get '/priority_queues'
      assert_equal 200, last_response.status
      assert_match(/default/, last_response.body)
    end
  end
end
