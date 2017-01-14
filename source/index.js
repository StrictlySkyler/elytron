import { log } from '../lib/logger';
import config from './config';
import { decorate, produce, get_producer_client } from './methods';
import { Consumer } from './consumer';
import Producer from './producer';
import Kafka from 'node-rdkafka';

const TOPICS = config.topics;
let producer_topic_settings = config.topic_settings.producer;
let consumer_topic_settings = config.topic_settings.consumer;
let producer_settings;
let consumer_settings = producer_settings = config.general_settings;
producer_settings['queue.buffering.max.ms'] =
  config.producer_settings['queue.buffering.max.ms'];
producer_settings['message.send.max.retries'] =
  config.producer_settings['message.send.max.retries'];
consumer_settings['fetch.error.backoff.ms'] =
  config.consumer_settings['fetch.error.backoff.ms'];

log(
  'Connecting to broker hosts:',
  config.general_settings['metadata.broker.list']
);


let _produce = new Producer(
  config, producer_settings, producer_topic_settings, TOPICS, Kafka
);
let consumer = new Consumer(
  consumer_settings, consumer_topic_settings, TOPICS, Kafka
);

decorate(_produce);

export { produce, consumer, get_producer_client };

