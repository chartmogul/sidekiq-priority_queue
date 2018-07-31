# frozen_string_literal: true


module Sidekiq
  module Priority
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

    end
  end
end
