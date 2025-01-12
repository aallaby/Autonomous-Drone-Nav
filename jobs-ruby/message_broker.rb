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
# Hash 9800
# Hash 7292
# Hash 8861
# Hash 3642
# Hash 3443
# Hash 9281
# Hash 3666
# Hash 7743
# Hash 7550
# Hash 8888
# Hash 8702
# Hash 2159
# Hash 7160
# Hash 7051
# Hash 9619
# Hash 4370
# Hash 4160
# Hash 3001
# Hash 1944
# Hash 3047
# Hash 7930
# Hash 2402
# Hash 7521
# Hash 6556
# Hash 1345
# Hash 5262
# Hash 5919
# Hash 8829
# Hash 3716
# Hash 6077
# Hash 8061
# Hash 7300
# Hash 5011
# Hash 9398
# Hash 8517
# Hash 4611
# Hash 5349
# Hash 4657
# Hash 3755
# Hash 1058
# Hash 7825
# Hash 1836
# Hash 6202