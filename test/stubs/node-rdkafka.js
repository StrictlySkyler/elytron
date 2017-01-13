/* eslint no-unused-vars: 0 */
import {
  set_active_topics,
  reset_handlers
} from '../../source/consumer/methods';

let producer_handlers = {};
let consumer_handlers = {};
let connected = false;
let producer_settings;
let consumer_settings;
let producer_topic_settings;
let consumer_topic_settings;
let produced_messages = [];
let consumed_topics = [];

class Client {
  on (event, callback) {
    this.type == 'producer' ?
      producer_handlers[event] = callback :
      consumer_handlers[event] = callback
    ;

    return this;
  }

  off (event, callback) {
    this.type == 'producer' ?
      delete producer_handlers[event] :
      delete consumer_handlers[event]
    ;
  }

  connect () {
    connected = true;
  }
}

class Producer extends Client {
  constructor (ProducerSettings, TopicSettings) {
    super();

    producer_settings = ProducerSettings;
    producer_topic_settings = TopicSettings;

    this.type = 'producer';

    return this;
  }

  produce (topic, partition, buffer) {
    if (
      ! topic ||
      ! buffer ||
      (partition !== null && typeof partition != 'number')
    ) {
      throw new TypeError('Invalid arguments!');
    }
    produced_messages.push({
      topic: topic,
      partition: partition,
      buffer: buffer
    });
  }

  poll () { return false; }
}

class KafkaConsumer extends Client {
  constructor (ConsumerSettings, TopicSettings) {
    super();

    consumer_settings = ConsumerSettings;
    consumer_topic_settings = TopicSettings;

    return this;
  }

  consume (topics) {
    return consumed_topics.concat(topics);
  }

  unsubscribe () {
    consumed_topics = [];
  }
}

let Kafka = {
  Producer: Producer,

  KafkaConsumer: KafkaConsumer,

  is_connected: function () { return connected; },

  get_producer_handlers: function () { return producer_handlers; },

  get_consumer_handlers: function () { return consumer_handlers; },

  get_produced_messages: function () { return produced_messages; },

  reset: function () {
    producer_handlers = {};
    consumer_handlers = {};
    producer_settings =
      producer_topic_settings =
      consumer_settings =
      consumer_topic_settings =
      null
    ;
    connected = false;
    produced_messages = [];
    consumed_topics = [];
    reset_handlers();
    set_active_topics([]);
  },

  ready: function () {
    if (producer_handlers.ready) { producer_handlers.ready(); }
    if (consumer_handlers.ready) { consumer_handlers.ready(); }
  }

};

export default Kafka;
