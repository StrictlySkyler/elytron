import BrokerError from './error';
import uuid from 'uuid';

let config;
let _produce;
let consumer;
let LONG_RUNNING_TOPIC_WAIT_MS;

let decorate = function (Config, Produce, Consumer) {

  config = Config;
  LONG_RUNNING_TOPIC_WAIT_MS = config.long_running_topic_wait_ms;
  _produce = Produce;
  consumer = Consumer;

};

let validate_arguments = function (topic, message) {

  let reason = 'Invalid arguments!\n';

  if (! topic || ! message || ! _.isObject(message) || _.isArray(message)) {
    reason +=
      'Both `topic` and `message` are required to produce a message.\n' +
      'Additionally, `message` must be an object.';
    throw new BrokerError(reason);
  }

};

let produce = function (args) {

  let topic = args.topic;
  let message = args.message;

  validate_arguments(topic, message);

  console.log(
    'Attempting to broker topic:\n',
    topic,
    '\nwith message:\n',
    message
  );

  let results = _produce(topic, message);
  return results;

};

export { decorate, produce };

