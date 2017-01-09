import BrokerError from './error';
import { log } from '../lib/logger';

let _produce;

let decorate = function (Produce) { return _produce = Produce; };

let validate_arguments = function (topic, message) {

  let reason = 'Invalid arguments!\n';

  if (
    ! topic ||
    ! message ||
    ! message instanceof Object ||
    message instanceof Array
  ) {
    reason +=
      'Both `topic` and `message` are required to produce a message.\n' +
      'Additionally, `message` must be an object.';
    throw new BrokerError(reason);
  }

  return true;

};

let produce = function (args) {

  let topic = args.topic;
  let message = args.message;

  validate_arguments(topic, message);

  log(
    'Attempting to broker topic:\n',
    topic,
    '\nwith message:\n',
    message
  );

  let results = _produce(topic, message);
  return results;

};

export { decorate, produce, validate_arguments };

