import fs from 'fs';
import uuid from 'uuid';
import { log, error } from '../../lib/logger';
import { BrokerError } from '../../lib/error';
import { kafkacat, tmp, brokers } from '../../lib/run';
import { consume } from '../consume';

let spawn;

const await_response = (topic, id, work) => {
  const offset = 1; // Skip the initial message which creates the topic
  const exit = true; // Cleanup when we've received our response
  const response_topic = `response.${topic}.${id}`;
  produce(response_topic, null, null, () => { // Create the response topic
    consume(response_topic, work, { group: false, offset, exit });
  });

  return response_topic;
};

const handle_producer_error = (data) => error(
  `Producer logged an error: ${data.toString()}`
);

const handle_producer_data = (data) => log(
  `Producer logged some data: ${data.toString()}`
);

const handle_producer_close = (code, message_file_path, callback) => {
  log(`Producer exited with code: ${code}`);
  fs.unlink(message_file_path, (err) => {
    if (err) throw err;
    if (callback) callback();
    return true;
  });
};

const pipe_to_kafkacat = (produce_options, message_file_path, callback) => {
  const producer = spawn(kafkacat, produce_options);

  producer.stdout.on('data', handle_producer_data);
  producer.stderr.on('data', handle_producer_error);
  producer.on('close', code => handle_producer_close(
    code, message_file_path, callback
  ));

  return producer;
};

const produce = (topic, message, work, callback) => {
  if (! topic) throw new BrokerError('A topic argument is required!');

  const id = uuid.v4();
  const timestamp = Date.now();
  const payload = { id, timestamp, message };
  const message_file_path = `${tmp}/elytron.message.${topic}.${id}`;
  const produce_options = [
    '-P', '-T', '-b', brokers, '-t', topic, message_file_path
  ];

  if (work) {
    payload.response_topic = await_response(topic, id, work);
    log(`Awaiting response on topic: ${payload.response_topic}`);
  }

  const message_string = JSON.stringify(payload);
  log(`Producing to ${topic} with ${message_string}`);
  fs.writeFile(message_file_path, message_string, (err) => {
    if (err) throw err;
    return pipe_to_kafkacat(produce_options, message_file_path, callback);
  });

  return { payload };
};

const decorate_producer = (Spawn) => {
  spawn = Spawn;
};

export { produce, decorate_producer };
