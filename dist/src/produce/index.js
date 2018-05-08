'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.decorate_producer = exports.produce = undefined;

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

var _logger = require('../../lib/logger');

var _error = require('../../lib/error');

var _run = require('../../lib/run');

var _consume = require('../consume');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var spawn = void 0;
var broker_list = void 0;

var await_response = function await_response(topic, id, work) {
  var offset = 1; // Skip the initial message which creates the topic
  var exit = true; // Cleanup when we've received our response
  var response_topic = 'response.' + topic + '.' + id;
  produce(response_topic, null, null, function () {
    // Create the response topic
    (0, _consume.consume)(response_topic, work, { group: false, offset: offset, exit: exit });
  }, broker_list);

  return response_topic;
};

var handle_producer_error = function handle_producer_error(data, callback) {
  var error_string = 'Producer logged an error: ' + data.toString();
  if (callback) return (0, _logger.error)(error_string);
  throw new _error.BrokerError('Producer logged an error: ' + data.toString());
};

var handle_producer_data = function handle_producer_data(data) {
  return (0, _logger.log)('Producer logged some data: ' + data.toString());
};

var handle_producer_close = function handle_producer_close(code, message_file_path, callback) {
  var out = code == 0 ? _logger.log : _logger.error;
  out('Producer exited with code: ' + code);
  _fs2.default.unlink(message_file_path, function (err) {
    if (err) throw err;
    return true;
  });
  if (callback) callback(code);
  return code;
};

var pipe_to_kafkacat = function pipe_to_kafkacat(produce_options, message_file_path, callback) {
  var producer = spawn(_run.kafkacat, produce_options);

  producer.stdout.on('data', handle_producer_data);
  producer.stderr.on('data', function (data) {
    return handle_producer_error(data, callback);
  });
  producer.on('close', function (code) {
    return handle_producer_close(code, message_file_path, callback);
  });

  return producer;
};

var produce = function produce(topic, message, work, callback, broker_string) {
  if (!topic) throw new _error.BrokerError('A topic argument is required!');
  broker_list = broker_string || _run.brokers;

  var id = _uuid2.default.v4();
  var timestamp = Date.now();
  var payload = { id: id, timestamp: timestamp, message: message };
  var message_file_path = _run.tmp + '/elytron.message.' + topic + '.' + id;
  var produce_options = ['-P', '-T', '-b', broker_list, '-t', topic, message_file_path];

  if (work) {
    payload.response_topic = await_response(topic, id, work);
    (0, _logger.log)('Awaiting response on topic: ' + payload.response_topic);
  }

  var message_string = JSON.stringify(payload);
  (0, _logger.log)('Producing to ' + topic + ' with ' + message_string);
  _fs2.default.writeFile(message_file_path, message_string, function (err) {
    if (err) throw err;
    return pipe_to_kafkacat(produce_options, message_file_path, callback);
  });

  return { payload: payload };
};

var decorate_producer = function decorate_producer(Spawn) {
  spawn = Spawn;
};

exports.produce = produce;
exports.decorate_producer = decorate_producer;