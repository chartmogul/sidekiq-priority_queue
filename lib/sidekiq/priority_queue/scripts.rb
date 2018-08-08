# frozen_string_literal: true


module Sidekiq
  module PriorityQueue
    module Scripts

      ZPOPMIN = %q(
        local resp = redis.call('zrevrange', KEYS[1], '0', '0')
        if (resp[1] ~= nil) then
          local val = resp[# resp]
          redis.call('zrem', KEYS[1], val)
          return val
        else
          return false
        end
      )

      ZPOPMIN_SADD = %q(
        local resp = redis.call('zrevrange', KEYS[1], '0', '0')
        if (resp[1] ~= nil) then
          local val = resp[# resp]
          redis.call('zrem', KEYS[1], val)
          redis.call('sadd', KEYS[2], val)
          return val
        else
          return false
        end
      )

    end
  end
end
