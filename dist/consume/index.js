'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.starve = exports.consume = undefined;

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

var _child_process = require('child_process');

var _run = require('../../lib/run');

var _logger = require('../../lib/logger');

var _error = require('../../lib/error');

var _produce = require('../produce');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var registered_consumers = {};

var list_topics = function list_topics() {
  var metadata = (0, _run.run)(_run.kafkacat, ['-L', // List broker metadata
  '-b', kafka_broker_host_string, '-J'] // Output as JSON
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

var consume = function consume(topic, work) {
  var offset = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 'beginning';
  var exit = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;


  if (topic instanceof Array && topic.length) {
    var new_consumers = [];
    topic.forEach(function (item) {
      new_consumers.push(consume(item, work, offset, exit));
    });

    return new_consumers;
  }

  if (!topic || typeof topic != 'string') throw new _error.BrokerError('A topic argument is required!  It must be either an Array or String.');

  if (topic === '*') return consume(list_topics(), work, offset, exit);

  var id = _uuid2.default.v4();
  var delimiter = ':msg:';
  var consume_options = ['-C', // Consume
  '-b', kafka_broker_host_string, '-t', topic, '-D', // Use a delimiter for messages
  delimiter, // The delimiter to use
  '-o', // Offset
  offset, '-u', '-E'];

  (0, _logger.log)('Consuming ' + topic + ' at offset ' + offset);
  var consumer = (0, _child_process.spawn)(kafkacat_path, consume_options);

  registered_consumers[topic] = registered_consumers[topic] || {};
  registered_consumers[topic][id] = consumer;

  consumer.stdout.on('data', function (data) {
    var parsed = data.toString().split(delimiter);
    (0, _logger.log)('Consumed data from ' + topic + ': ' + parsed);
    parsed.pop(); // Empty string after delimiter
    parsed.forEach(function (item) {
      var results = work(item);

      if (item.response_topic) (0, _produce.produce)(item.response_topic, results);
    });

    if (exit) teardown_consumer(topic, id);

    return parsed;
  });
  consumer.on('close', function (code) {
    (0, _logger.log)('Consumer exited with code ' + code);

    return code;
  });

  return { id: id, consumer: consumer };
};

var starve = function starve(topic, id) {
  if (!topic || typeof topic != 'string') throw new _error.BrokerError('A topic argument is required!  It must be either an Array or String.');

  if (id) return teardown_consumer(topic, id);

  Object.keys(registered_consumers[topic]).forEach(function (consumer_id) {
    teardown_consumer(topic, consumer_id);
  });

  delete registered_consumers[topic];

  return registered_consumers;
};

exports.consume = consume;
exports.starve = starve;