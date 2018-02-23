'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.brokers = exports.tmp = exports.kafkacat = exports.run = undefined;

var _os = require('os');

var _os2 = _interopRequireDefault(_os);

var _child_process = require('child_process');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var run = function run(command, flags) {
  var encoding = 'utf8';
  var results = (0, _child_process.spawnSync)(command, flags, { encoding: encoding }).stdout.replace('\n', '');

  return results;
};

var kafkacat = run('which', ['kafkacat']);
var tmp = _os2.default.tmpdir();
var brokers = process.env.KAFKA_BROKERS || 'localhost';

exports.run = run;
exports.kafkacat = kafkacat;
exports.tmp = tmp;
exports.brokers = brokers;