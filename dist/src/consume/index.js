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
var restart_consumer_interval = process.env.KAFKA_RESTART_CONSUMER_INTERVAL_MS || 1000;

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

  (0, _logger.log)('Consumer teardown: ' + topic + ' ' + id);
  target_consumer.stdout.destroy();
  target_consumer.stderr.destroy();
  target_consumer.kill();

  delete registered_consumers[topic][id];

  return registered_consumers[topic];
};

var consume_multi_topics = function consume_multi_topics(topics, work, options) {
  var new_consumers = [];
  topics.forEach(function (topic) {
    new_consumers.push(consume(topic, work, options));
  });

  return new_consumers;
};

var handle_consumer_data = function handle_consumer_data(data, topic, id, work, exit) {
  var parsed = data.split(delimiter).reverse();
  // Remove any empty string after the delimiter
  if (parsed[0] === '') parsed.shift();

  var i = parsed.length;

  while (i--) {
    try {
      // Attempt to parse our most recent chunk
      var deserialized = JSON.parse(parsed[i]);
      var payload = void 0;

      (0, _logger.log)('Consumed data from ' + topic + ': ' + deserialized.payload);
      var results = work(deserialized.payload);

      try {
        payload = JSON.parse(deserialized.payload);
      } catch (e) {
        payload = deserialized.payload;
      }

      if (payload.response_topic) (0, _produce.produce)(payload.response_topic, results);
    } catch (err) {
      // Incomplete chunk from string, save for next event
      (0, _logger.error)('Unable to parse data chunk: ' + parsed[i], err);
      break;
    }

    parsed.splice(i, 1);
  }

  if (exit) teardown_consumer(topic, id);

  return parsed.join('');
};

var handle_consumer_error = function handle_consumer_error(err, topic, id) {
  var disconnect_msg = 'Broker transport failure';
  (0, _logger.error)('Received error from consumer: ' + err);

  if (err.match(disconnect_msg)) {
    (0, _logger.log)('Attempting to reconnect...');
    teardown_consumer(topic, id);
  }
};

var handle_consumer_close = function handle_consumer_close(code, topic, work, options) {
  var msg = 'Consumer exited with code ' + code;

  (0, _logger.log)(msg);
  if (!options.exit) {
    (0, _logger.log)('Restarting consumer...');
    setTimeout(consume, restart_consumer_interval, topic, work, options);
  } else throw new _error.BrokerError(msg);

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
  var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {
    group: false, offset: 'end', exit: false
  };
  var _options$group = options.group,
      group = _options$group === undefined ? false : _options$group,
      _options$offset = options.offset,
      offset = _options$offset === undefined ? 'beginning' : _options$offset,
      _options$exit = options.exit,
      exit = _options$exit === undefined ? false : _options$exit;

  if (validate_arguments(topic, work).multi && !group) return consume_multi_topics(topic, work, options);

  if (topic === '*') return consume(list_topics(), work, options);

  var refresh_interval = process.env.KAFKA_TOPIC_METADATA_REFRESH_INTERVAL_MS || 60000;
  var consumer_type = group ? ['-G', group, topic instanceof Array ? topic.join(' ') : topic] : ['-C', '-t', topic];
  var id = _uuid2.default.v4();
  var consume_options = ['-b', _run.brokers, '-D', delimiter, '-o', offset, '-u', '-J', '-X', 'topic.metadata.refresh.interval.ms=' + refresh_interval].concat(consumer_type);

  var stdout = '';
  var stale_cache_timer = void 0;

  (0, _logger.log)('Consuming ' + topic + ' at offset ' + offset);
  var consumer = spawn(_run.kafkacat, consume_options);

  consumer.stdout.on('data', function (data) {
    clearTimeout(stale_cache_timer);
    stdout += data.toString();
    stdout = handle_consumer_data(stdout, topic, id, work, exit);
    stale_cache_timer = setTimeout(function () {
      return stdout = '';
    }, process.env.ELYTRON_STALE_CACHE_TIMER || refresh_interval);
  });
  consumer.stderr.on('data', function (data) {
    handle_consumer_error(data.toString(), topic, id);
  });

  consumer.on('close', function (code) {
    handle_consumer_close(code, topic, work, options);
  });

  return register_consumer(consumer, topic, id);
};

var starve = function starve(topic, id) {
  if (!topic || typeof topic != 'string') throw new _error.BrokerError('A topic argument is required!  It must be either an Array or String.');

  if (topic == '*') {
    Object.keys(registered_consumers).forEach(function (consumed_topic) {
      starve(consumed_topic);
    });
    return registered_consumers;
  }

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