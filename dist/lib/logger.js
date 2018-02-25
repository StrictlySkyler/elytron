'use strict';

var debug = require('debug');

var log = debug('elytron:log');
var error = debug('elytron:error');

module.exports = { log: log, error: error };