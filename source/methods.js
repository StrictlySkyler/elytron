import BrokerError from './error';
import { log, error } from '../lib/logger';
import uuid from 'uuid';
import { set_timer, clear_timer, get_client } from './producer/methods';

let config;
let _produce;
let get_producer_client = get_client;
let consumer;
let LONG_RUNNING_TOPIC_WAIT_MS;

let decorate = function (Config, Produce, Consumer) {

  config = Config;
  LONG_RUNNING_TOPIC_WAIT_MS = config.long_running_topic_wait_ms;
  _produce = Produce;
  consumer = Consumer;
};

let validate_arguments = function (topic, message, work) {

  let reason = 'Invalid arguments!\n';

  if (
    ! topic ||
    (message && (! message instanceof Object || message instanceof Array))
  ) {
    reason +=
      '\tA `topic` is required to produce a message.\n' +
      '\tAdditionally, if provided, `message` must be an object.';
    throw new BrokerError(reason);
  }

  if (work && ! typeof work == 'function') {
    reason +=
      '\tIf either `awaiting` or `consumer_work` arguments are passed,\n' +
      '\tthe other must also be present.\n' +
      '\tAlso, `awaiting` must be boolean, and `consumer_work` a function.';
    throw new BrokerError(reason);
  }
  return true;

};

let await_response = function (topic, message, work) {

  let awaiting_hash = uuid.v4();
  let awaiting_topic = topic + '.' + awaiting_hash;
  let long_running;

  log('Creating awaiting topic:', awaiting_topic);
  _produce(awaiting_topic);

  consumer.consume([awaiting_topic]);
  consumer
    .on('data', function (data) {

      clear_timer(long_running);

      log(
        'Consumed message from topic:',
        awaiting_topic,
        '\n with data:',
        data.value.toString()
      );

      consumer.unsubscribe(awaiting_topic);

      let parsed_message = data.value.toString();
      work(parsed_message);
    })
    .on('error', function (err) {

      clear_timer(long_running);
      error('Awaiting consumer error:\n', err);

      consumer.unsubscribe(awaiting_topic);
    })
  ;

  long_running = set_timer(function () {

    error(
      'Disconnected from long-running consumer topic:',
      awaiting_topic,
      '\n after LONG_RUNNING_TOPIC_WAIT_MS:',
      LONG_RUNNING_TOPIC_WAIT_MS
    );

    consumer.unsubscribe(awaiting_topic);

  }, LONG_RUNNING_TOPIC_WAIT_MS);

  message.awaiting_topic = awaiting_topic;
  return message;
};

let produce = function (topic, message, work) {

  validate_arguments(topic, message, work);

  log(
    'Attempting to broker topic:\n',
    topic,
    '\nwith message:\n',
    message
  );

  if (work) { await_response(topic, message, work); }

  let results = _produce(topic, message);
  return results;

};

export {
  decorate, produce, validate_arguments, await_response, get_producer_client
};

