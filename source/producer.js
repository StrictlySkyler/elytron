import { log, error } from '../lib/logger';
import {
  set_producer_retry_ms,
  set_client,
  received,
  produce,
  register_topics
} from './producer/methods';

let producer;

export default class Producer {
  constructor (config, producer_settings, topic_settings, topics, Kafka) {

    producer = new Kafka.Producer(
      producer_settings, topic_settings
    );

    set_producer_retry_ms(config.producer_retry_ms);
    set_client(producer);

    log('Kafka producer built, registering topics...');
    producer
      .on('ready', function () {
        register_topics(topics);
      })

      .on('delivery-report', function (report) {
        if (! report && arguments.length > 1) { report = arguments[1]; }
        if (received[report.topic]) {
          received[report.topic].awaiting_report = false;
        }
      })

      .on('event', function (event) {
        log('Producer event:\n', event);
      })
      .on('error', function (err) {
        error('Producer error:\n', err);
      })
    ;

    producer.connect();

    return produce;

  }
}
