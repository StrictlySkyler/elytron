var debug = require('debug');

var log = debug('cockroach:log');
var error = debug('cockroach:error');

module.exports = { log: log, error: error };
