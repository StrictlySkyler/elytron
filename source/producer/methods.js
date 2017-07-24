import uuid from 'uuid';
import BrokerError from '../error';
import debug from 'debug';

let log = debug('elytron:log');
let error = debug('elytron:error');

let PRODUCER_RETRY_MS;
let producer;
let received = {};

let set_producer_retry_ms = function (ms) {
  return PRODUCER_RETRY_MS = ms;
};

let set_client = function (client) {
  return producer = client;
};

let get_client = function () {
  return producer;
};

let set_received = function (hash) {
  received = hash;
};

let produce_once = function (topic, value, buffer) {
  log(
    'Producing to topic:',
    topic,
    '\n with message:',
    value
  );

  received[topic] = {
    awaiting_report: true
  };

  try {
    producer.produce(topic, null, buffer);
    return true;

  } catch (err) {
    error(err);
    return false;
  }
};

let poll_producer = function (topic, message_package) {
  if (! topic || ! message_package) {
    throw new TypeError('Invalid Arguments!');
  }

  log(
    'No delivery report receieved for topic:',
    topic,
    '\n after',
    PRODUCER_RETRY_MS,
    'ms, polling producer.'
  );

  if (producer.poll() !== false) {
    received[topic].timer = produce_at_least_once(topic, message_package);
  }

  return topic;
};

let set_timer = function (callback, ms) {
  if (typeof Meteor != 'undefined') return Meteor.setTimeout(callback, ms);
  return setTimeout(callback, ms);
};

let clear_timer = function (timer) {
  if (typeof Meteor != 'undefined') return Meteor.clearTimeout(timer);
  return clearTimeout(timer);
};

let acknowledge_delivery_report = function (topic) {
  log('Delivery report received for topic:', topic);
  clear_timer(received[topic].timer);
  delete received[topic];

  return received;
};

let produce_at_least_once = function (topic, message_package) {

  if (! topic || ! message_package) {
    throw new TypeError('Invalid arguments!');
  }

  let package_buffer = Buffer.from(JSON.stringify(message_package));
  let is_new;

  if (! received[topic]) {
    is_new = true;
    produce_once(topic, message_package.value, package_buffer);
  }

  received[topic].timer = set_timer(function () {

    if (received[topic] && received[topic].awaiting_report) {
      poll_producer(topic, message_package);

    } else if (received[topic] && received[topic].awaiting_report === false) {
      acknowledge_delivery_report(topic);

    } else {
      error(
        'WARNING:\n',
        'Events are being produced faster than we are checking for delivery\n',
        'reports.  Consider reducing the "PRODUCER_RETRY_MS" variable to\n',
        'accomodate.  Currently:\n',
        'PRODUCER_RETRY_MS:', PRODUCER_RETRY_MS
      );
    }
  }, PRODUCER_RETRY_MS);

  return is_new ? received[topic] : received[topic].timer;
};

let produce = function (topic, message) {
  let timestamp = Date.now();

  if (! topic || typeof topic != 'string') {
    throw new BrokerError(
      'Invalid topic argument!\n' +
      'The first argument must be a string,\n' +
      'and should be the name of a valid Kafka topic.'
    );
  }
  if (! message) { message = { registration: timestamp }; }

  let id = uuid.v4();
  let message_package = {
    timestamp: timestamp,
    id: id,
    value: message
  };

  let results = produce_at_least_once(topic, message_package);
  let status_hash = {
    package: message_package,
    results: results
  };

  if (results.awaiting_report || results.timer) {
    status_hash.submitted = true;
    return status_hash;
  }

  status_hash.submitted = false;
  return status_hash;
};

let register_topics = function (topics) {
  log('Producer ready.  Registering on topics.');
  // Setup initial topics we're interested in consuming:
  // If a topic doesn't exist, we need to write to it before we consume it
  _.each(topics, function (topic) {
    try {
      produce(topic);
      log('Topic registered:', topic);
    } catch (err) {
      error('Error registering topic:\n', err);
    }
  });
};

export {
  PRODUCER_RETRY_MS,
  received,
  produce_once,
  poll_producer,
  set_timer,
  clear_timer,
  acknowledge_delivery_report,
  produce_at_least_once,
  produce,
  register_topics,
  set_producer_retry_ms,
  set_client,
  get_client,
  set_received
};
