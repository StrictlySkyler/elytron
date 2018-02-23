'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.produce = undefined;

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

var _logger = require('../../lib/logger');

var _error = require('../../lib/error');

var _run = require('../../lib/run');

var _consume = require('../consume');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var produce = function produce(topic, message, work) {
  if (!topic) throw new _error.BrokerError('A topic argument is required!');

  var response_consumer = void 0;

  var id = _uuid2.default.v4();
  var timestamp = Date.now();
  var payload = { id: id, timestamp: timestamp, message: message };

  _fs2.default.writeFileSync(message_file_path, message_string);

  var produce_options = ['-P', // Produce
  '-T', // Output sent messages to stdout, acting like tee.
  '-b', // Kafka broker host string: host[:port][,host[:port],...]
  _run.brokers, '-t', // Topic
  topic, message_file_path];

  if (work) {
    payload.response_topic = 'response.' + topic + '.' + id;

    produce(payload.response_topic);
    response_consumer = (0, _consume.consume)(payload.response_topic, work, 1, true);
    (0, _logger.log)('Awaiting response on topic: ' + response_topic);
  }

  var message_string = JSON.stringify(payload);
  var message_file_path = tmp + '/elytron.message.' + topic + '.' + id;

  (0, _logger.log)('Producing to ' + topic + ' with ' + message_string);
  var results = (0, _run.run)(_run.kafkacat, produce_options);

  _fs2.default.unlinkSync(message_file_path);

  return { results: results, response_consumer: response_consumer };
};

exports.produce = produce;