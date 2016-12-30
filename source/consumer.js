import Kafka from 'node-rdkafka';

let consumer;
let TOPICS;
let active_topics = [];
let handlers = {};
let produce;

let check_handlers = function (data, parsed_message) {

  console.log('Checking handlers for:', data.topic);
  if (! parsed_message.value.registration && handlers[data.topic]) {
    return true;
  }

  return false;
};

let send_results = function (topic, results) {
  console.log('Sending results back to awaiting topic:', topic);

  let results_string = JSON.stringify(results);
  produce(parsed_message.awaiting_topic, results_string);
};

let handle_message = function (topic, message) {
  console.log('Calling handler for:', topic);
  return handlers[topic](message);
};

export default class Consumer {
  constructor (consumer_settings, topic_settings, topics, Produce) {

    consumer = new Kafka.KafkaConsumer(
      consumer_settings, topic_settings
    );

    TOPICS = topics;
    produce = Produce;

    consumer
      .on('ready', () => {
        console.log('Consumer ready.');
        this.consume(TOPICS);
      })

      .on('data', function (data) {

        let results;
        let parsed_message = JSON.parse(data.value.toString());
        console.log(
          'Consuming message from topic:',
          data.topic,
          '\n with data:\n',
          parsed_message
        );

        if (check_handlers(data, parsed_message)) {
          results = handle_message(data.topic, parsed_message);
        }

      })

      .on('error', function (err) {
        console.error('Consumer error:\n', err);
      })
      .on('event.log', function () {
        console.log('Event log:\n', arguments);
      })
    ;

    consumer.connect();

    return this;

  }

  consume (topics) {
    active_topics = active_topics.concat(topics);

    if (active_topics.length) {
      console.log('Consuming topics:\n', active_topics);
      consumer.consume(active_topics);
    }

    return this;
  }

  unsubscribe () {
    consumer.unsubscribe();

    handlers = {};

    return this;
  }

  on (event, callback) {
    consumer.on(event, callback);

    return this;
  }

  topics (topic_hash) {
    console.log('Topic hash received, assigning handlers.');
    let topic_list = [];
    _.each(topic_hash, function (value, key) {
      handlers[key] = value;
      topic_list.push(key);
    });

    this.consume(topic_list);
  }
}
