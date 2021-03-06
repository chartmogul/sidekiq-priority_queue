# frozen_string_literal: true

require 'sidekiq'
require 'sidekiq/priority_queue/api'
require 'sidekiq/priority_queue/client'
require 'sidekiq/priority_queue/combined_fetch'
require 'sidekiq/priority_queue/fetch'
require 'sidekiq/priority_queue/reliable_fetch'
require 'sidekiq/priority_queue/scripts'
require 'sidekiq/priority_queue/web'

module Sidekiq
  module PriorityQueue
  end
end
