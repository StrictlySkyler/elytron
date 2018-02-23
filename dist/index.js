'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.starve = exports.consume = exports.produce = undefined;

var _child_process = require('child_process');

var _fs = require('fs');

var _fs2 = _interopRequireDefault(_fs);

var _os = require('os');

var _os2 = _interopRequireDefault(_os);

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

var _logger = require('../lib/logger');

var _error = require('../lib/error');

var _produce = require('./produce');

var _consume = require('./consume');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

//TODO: tests

(0, _logger.log)('Loading elytron using broker host string: ' + kafka_broker_host_string);

exports.produce = _produce.produce;
exports.consume = _consume.consume;
exports.starve = _consume.starve;