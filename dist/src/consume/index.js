'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.decorate_consumer = exports.starve = exports.consume = undefined;

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

var _run = require('../../lib/run');

var _logger = require('../../lib/logger');

var _error = require('../../lib/error');

var _produce = require('../produce');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var registered_consumers = {};
var run = void 0;
var spawn = void 0;

var delimiter = ':msg:';

var decorate_consumer = function decorate_consumer(Run, Spawn) {
  run = Run;
  spawn = Spawn;
};

var list_topics = function list_topics() {
  var metadata = run(_run.kafkacat, ['-L', // List broker metadata
  '-b', _run.brokers, '-J'] // Output as JSON
  );
  var parsed_metadata = JSON.parse(metadata);
  var all_topics = parsed_metadata.topics.map(function (item) {
    return item.topic;
  });

  return all_topics;
};

var teardown_consumer = function teardown_consumer(topic, id) {
  var target_consumer = registered_consumers[topic][id];

  target_consumer.stdout.destroy();
  target_consumer.stderr.destroy();

  delete registered_consumers[topic][id];

  return registered_consumers[topic];
};

var consume_multi_topics = function consume_multi_topics(topics, work, offset, exit) {
  var new_consumers = [];
  topics.forEach(function (topic) {
    new_consumers.push(consume(topic, work, offset, exit));
  });

  return new_consumers;
};

var handle_consumer_data = function handle_consumer_data(data, topic, id, work, exit) {
  var parsed = data.toString().split(delimiter);
  (0, _logger.log)('Consumed data from ' + topic + ': ' + parsed);
  parsed.pop(); // Empty string after delimiter
  parsed.forEach(function (item) {
    var results = work(item);
    var deserialized = {};

    try {
      deserialized = JSON.parse(item);
    } catch (err) {
      deserialized.response_topic = false;
    }

    if (deserialized.response_topic) (0, _produce.produce)(deserialized.response_topic, results);
  });

  if (exit) teardown_consumer(topic, id);

  return parsed;
};

var handle_consumer_error = function handle_consumer_error(err) {
  var parsed = err.toString();
  (0, _logger.error)('Received error from consumer: ' + parsed);
  return parsed;
};

var handle_consumer_close = function handle_consumer_close(code) {
  (0, _logger.log)('Consumer exited with code ' + code);
  return code;
};

var register_consumer = function register_consumer(consumer, topic, id) {
  registered_consumers[topic] = registered_consumers[topic] || {};
  registered_consumers[topic][id] = consumer;

  return registered_consumers[topic];
};

var validate_arguments = function validate_arguments(topic, work) {
  if (topic instanceof Array && topic.length) {
    return { multi: true };
  }

  if (!topic || typeof topic != 'string') throw new _error.BrokerError('A topic argument is required!  It must be either an Array or String.');

  if (!work || typeof work != 'function') throw new _error.BrokerError('A work argument is required!  It must be a function.');

  return true;
};

var consume = function consume(topic, work) {
  var offset = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 'beginning';
  var exit = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;

  if (validate_arguments(topic, work).multi) return consume_multi_topics(topic, work, offset, exit);

  if (topic === '*') return consume(list_topics(), work, offset, exit);

  var id = _uuid2.default.v4();
  var consume_options = ['-C', '-b', _run.brokers, '-t', topic, '-D', delimiter, '-o', offset, '-u'];

  (0, _logger.log)('Consuming ' + topic + ' at offset ' + offset);
  var consumer = spawn(_run.kafkacat, consume_options);
  consumer.stdout.on('data', function (data) {
    return handle_consumer_data(data, topic, id, work, exit);
  });
  consumer.stderr.on('data', function (data) {
    return handle_consumer_error(data, topic, work, exit);
  });
  consumer.on('close', handle_consumer_close);

  return register_consumer(consumer, topic, id);
};

var starve = function starve(topic, id) {
  if (!topic || typeof topic != 'string') throw new _error.BrokerError('A topic argument is required!  It must be either an Array or String.');

  if (!registered_consumers[topic] || id && !registered_consumers[topic][id]) throw new _error.BrokerError('No consumer to starve!');

  if (id) return teardown_consumer(topic, id);

  Object.keys(registered_consumers[topic]).forEach(function (consumer_id) {
    teardown_consumer(topic, consumer_id);
  });

  delete registered_consumers[topic];

  return registered_consumers;
};

exports.consume = consume;
exports.starve = starve;
exports.decorate_consumer = decorate_consumer;