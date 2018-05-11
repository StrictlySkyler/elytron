'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.starve = exports.consume = exports.produce = undefined;

var _child_process = require('child_process');

var _logger = require('../lib/logger');

var _produce = require('./produce');

var _consume = require('./consume');

var _run = require('../lib/run');

(0, _logger.log)('Loading elytron using broker host string: ' + _run.brokers);
(0, _produce.decorate_producer)(_child_process.spawn);
(0, _consume.decorate_consumer)(_run.run, _child_process.spawn);

process.on('SIGINT', function () {
  process.exit();
});
process.on('SIGTERM', function () {
  process.exit();
});
process.on('exit', function () {
  (0, _consume.starve)('*');
});

exports.produce = _produce.produce;
exports.consume = _consume.consume;
exports.starve = _consume.starve;