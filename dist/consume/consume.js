'use strict';

var _uuid = require('uuid');

var _uuid2 = _interopRequireDefault(_uuid);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

var produce = function produce(topic, message, work) {
  if (!topic) throw new BrokerError('A topic argument is required!');

  var response_consumer = void 0;

  var id = _uuid2.default.v4();
  var timestamp = Date.now();
  var payload = { id: id, timestamp: timestamp, message: message };
  var message_string = JSON.stringify(payload);
  var message_file_path = tmp + '/elytron.message.' + topic + '.' + id;

  fs.writeFileSync(message_file_path, message_string);

  var produce_options = ['-P', // Produce
  '-T', // Output sent messages to stdout, acting like tee.
  '-b', // Kafka broker host string: host[:port][,host[:port],...]
  kafka_broker_host_string, '-t', // Topic
  topic, message_file_path];

  log('Producing to ' + topic + ' with ' + message_string);
  var results = run(kafkacat_path, produce_options);

  fs.unlinkSync(message_file_path);

  if (work) {
    var response_topic = 'response.' + topic + '.' + id;

    produce(response_topic);
    response_consumer = consume(response_topic, work, 1, true);
    log('Awaiting response on topic: ' + response_topic);
  }

  return { results: results, response_consumer: response_consumer };
};