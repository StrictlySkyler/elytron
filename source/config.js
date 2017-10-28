import os from 'os';
import uuid from 'uuid';
import EJSON from 'ejson';

let uuid_suffix = process.env.BROKER_LOG == 'full' ? '_' + uuid.v4() : '';
let env = process.env;
const config = {
  topics: process.env.TOPICS ? EJSON.parse(process.env.TOPICS) : [],
  producer_retry_ms: process.env.PRODUCER_RETRY_MS || 1000,
  long_running_topic_wait_ms: process.env.LONG_RUNNING_TOPIC_WAIT_MS || 20000,
  general_settings: {
    'group.id': env.GROUP_ID || 'group_' + os.hostname() + uuid_suffix,
    'client.id': env.CLIENT_ID || 'client_' + os.hostname() + uuid_suffix,
    'metadata.broker.list': env.BROKER_LIST || 'localhost:9092',
    'socket.timeout.ms': env.SOCKET_TIMEOUT_MS || 1000,
    'coordinator.query.interval.ms': env.COORDINATOR_QUERY_INTERVAL_MS || 1000,
    // Smaller than this tends to not work          vvvvv
    'session.timeout.ms': env.SESSION_TIMEOUT_MS || 10000,
    'metadata.request.timeout.ms': env.METADATA_REQUEST_TIMEOUT_MS || 1000,
    'topic.metadata.refresh.interval.ms':
      env.TOPIC_METADATA_REFRESH_INTERVAL_MS || 1000,
    'topic.metadata.refresh.sparse':
      env.TOPIC_METADATA_REFRESH_INTERVAL_MS || false,
    'log_level': env.LOG_LEVEL || 0,
    'socket.blocking.max.ms': env.SOCKET_BLOCKING_MAX_MS || 1000,
    'socket.keepalive.enable': env.SOCKET_KEEPALIVE_ENABLE || true,
    'socket.max.fails': env.SOCKET_MAX_FAILS || 0,
    'reconnect.backoff.jitter.ms': env.RECONNECT_BACKOFF_JITTER_MS || 100,
    'event_cb': true,
    'dr_cb': true
  },
  producer_settings: {
    'queue.buffering.max.ms': env.PRODUCER_QUEUE_BUFFERING_MAX_MS || 100,
    'message.send.max.retries': env.PRODUCER_MESSAGE_SEND_MAX_RETRIES || 10
  },
  consumer_settings: {
    'fetch.error.backoff.ms': env.CONSUMER_MESSAGE_SEND_MAX_RETRIES || 100
  },
  topic_settings: {
    producer: {
      'request.timeout.ms': env.TOPIC_PRODUCER_REQUEST_TIMEOUT_MS || 1000,
      'message.timeout.ms': env.TOPIC_PRODUCER_MESSAGE_TIMEOUT || 1000,
      'request.required.acks': env.TOPIC_PRODUCER_REQUIRED_ACKS || 1
    },
    consumer: {
      'offset.store.sync.interval.ms':
        env.TOPIC_CONSUMER_OFFSET_STORE_SYNC_INTERVAL_MS || 1000,
      'auto.offset.reset': env.TOPIC_CONSUMER_AUTO_OFFSET_RESET || 'earliest'
    }
  }
};

export default config;
