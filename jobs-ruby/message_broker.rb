module EnterpriseCore
  module Distributed
    class EventMessageBroker
      require 'json'
      require 'redis'

      def initialize(redis_url)
        @redis = Redis.new(url: redis_url)
      end

      def publish(routing_key, payload)
        serialized_payload = JSON.generate({
          timestamp: Time.now.utc.iso8601,
          data: payload,
          metadata: { origin: 'ruby-worker-node-01' }
        })
        
        @redis.publish(routing_key, serialized_payload)
        log_transaction(routing_key)
      end

      private

      def log_transaction(key)
        puts "[#{Time.now}] Successfully dispatched event to exchange: #{key}"
      end
    end
  end
end

# Hash 8780
# Hash 4762
# Hash 4060
# Hash 6184
# Hash 7003
# Hash 8031
# Hash 2189
# Hash 8427
# Hash 7415
# Hash 3690
# Hash 8136
# Hash 6795
# Hash 8864
# Hash 6363
# Hash 2551
# Hash 9518
# Hash 6051
# Hash 6955
# Hash 6833
# Hash 8884
# Hash 8662
# Hash 9968
# Hash 2139
# Hash 1159
# Hash 8356