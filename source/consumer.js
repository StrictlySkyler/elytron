import { log, error } from '../lib/logger';
import {
  check_handlers,
  handle_message,
  get_consumed_topics,
  get_active_topics,
  set_active_topics,
  reset_handlers,
  get_topic_handlers,
  send_results
} from './consumer/methods';

let consumer;
let TOPICS;

class Consumer {
  constructor (consumer_settings, topic_settings, topics, Kafka) {
    if (! Kafka) { throw new TypeError('Invalid Arguments!'); }

    consumer = new Kafka.KafkaConsumer(
      consumer_settings, topic_settings
    );

    TOPICS = topics;

    consumer
      .on('ready', () => {
        log('Consumer ready.');
        this.consume(TOPICS);
      })

      .on('data', function (data) {

        let results;
        let parsed_message = JSON.parse(data.value.toString());
        log(
          'Consuming message from topic:',
          data.topic,
          '\n with data:\n',
          parsed_message
        );
        let handles = check_handlers(data, parsed_message);

        if (handles.length) {
          results = handle_message(handles, parsed_message);
        }

        if (parsed_message.awaiting_topic && results) {
          send_results(parsed_message.awaiting_topic, results);
        }

        return results;

      })

      .on('error', function (err) {
        error('Consumer error:\n', err);
      })
      .on('event.log', function () {
        log('Event log:\n', arguments);
      })
    ;

    consumer.connect();

    return this;

  }

  consume (topics) {
    if (topics && ! (topics instanceof Array)) {
      throw new TypeError(
        'Topics passed to this method must be an array of strings.'
      );
    }

    set_active_topics(get_active_topics().concat(topics));

    if (get_active_topics().length) {
      log('Consuming topics:\n', get_active_topics());
      consumer.consume(get_active_topics());
    }

    return this;
  }

  starve (topic) {
    let active_topics = get_active_topics();
    let handlers = get_topic_handlers();

    if (topic) {
      log('Unsubscribing from:', topic);
      let index = active_topics.indexOf(topic);
      active_topics = active_topics.splice(index, 1);

      this.consume(active_topics);
      set_active_topics(active_topics);
      delete handlers[topic]
    }
    else {
      log('Unsubscribing from all topics.');
      set_active_topics([]);
      reset_handlers();
    }

    consumer.unsubscribe();

    return this;
  }

  unsubscribe () {
    this.starve(arguments);
  }

  on (event, callback) {
    if (
      ! event ||
      ! callback ||
      ! event instanceof String ||
      ! callback instanceof Function
    ) { throw new TypeError('Invalid Arguments!'); }

    consumer.on(event, callback);

    return this;
  }

  off (event, callback) {
    consumer.off(event, callback);

    return this;
  }

  topic (topic, callback) {
    if (
      ! topic ||
      ! callback ||
      ! topic instanceof String ||
      ! callback instanceof Function
    ) { throw new TypeError('Invalid Arguments!'); }
    log('Topic callback received, assigning handler and consuming new topic.');
    get_topic_handlers()[topic] = callback;

    return this.consume([topic]);
  }

  topics (topic_hash) {
    if (
      (topic_hash && ! topic_hash instanceof Object) ||
      topic_hash instanceof Function ||
      topic_hash instanceof Array
    ) {
      throw new TypeError('Invalid Arguments!');
    }
    if (! topic_hash) { return get_consumed_topics(); }

    log('Topic hash received, assigning handlers and consuming new topics.');
    let topic_list = [];
    Object.keys(topic_hash).forEach(function (topic) {
      get_topic_handlers()[topic] = topic_hash[topic];
      topic_list.push(topic);
    });

    return this.consume(topic_list);
  }
}

export {
  Consumer,
  check_handlers,
  handle_message,
  get_consumed_topics
};
