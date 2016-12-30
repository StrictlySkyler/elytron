/* eslint no-unused-vars: 0 */

let producer_handlers = {};
let connected = false;
let producer_settings;
let topic_settings;
let produced_messages = [];

class Producer {
  constructor (ProducerSettings, TopicSettings) {
    producer_settings = ProducerSettings;
    topic_settings = TopicSettings;

    this.on = function (event, callback) {
      producer_handlers[event] = callback;
      return this;
    };

    this.connect = function () {
      connected = true;
    };

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

let Kafka = {
  Producer: Producer,

  is_connected: function () { return connected; },

  get_producer_handlers: function () { return producer_handlers; },

  get_produced_messages: function () { return produced_messages; },

  reset: function () {
    producer_handlers = {};
    producer_settings = topic_settings = null;
    connected = false;
    produced_messages = [];
  }
};

export default Kafka;
