import uuid from 'uuid';
import split from 'split';
import { kafkacat, brokers } from '../../lib/run';
import { log, error } from '../../lib/logger';
import { BrokerError } from '../../lib/error';
import { produce } from '../produce';

let registered_consumers = {};
let run;
let spawn;

const restart_consumer_interval = process.env
  .KAFKA_RESTART_CONSUMER_INTERVAL_MS ||
  1000
;

const decorate_consumer = (Run, Spawn) => {
  run = Run;
  spawn = Spawn;
};

const list_topics = () => {
  const metadata = run(kafkacat, [
    '-L', // List broker metadata
    '-b',
    brokers,
    '-J', // Output as JSON
  ]);
  const parsed_metadata = JSON.parse(metadata);
  const all_topics = parsed_metadata.topics.map(item => item.topic);

  return all_topics;
};

const teardown_consumer = (topic, id) => {
  const target_consumer = registered_consumers[topic][id];

  log(`Consumer teardown: ${topic} ${id}`);
  target_consumer.stdout.destroy();
  target_consumer.stderr.destroy();
  target_consumer.kill();

  delete registered_consumers[topic][id];

  return registered_consumers[topic];
};

const consume_multi_topics = (topics, work, options) => {
  let new_consumers = [];
  topics.forEach((topic) => {
    new_consumers.push(consume(topic, work, options));
  });

  return new_consumers;
};

const handle_consumer_data = (data, topic, id, work, exit) => {
  log(`Consumed data from ${topic}: ${data.payload}`);
  let results = work(JSON.parse(data.payload));

  if (data.response_topic) produce(payload.response_topic, results);

  if (exit) teardown_consumer(topic, id);

  return results;
};

const handle_consumer_error = (err, topic, id) => {
  let transport_msg = 'Broker transport failure';
  let host_msg = 'Host resolution failure';

  error(`Received error from consumer: ${err}`);

  if (err.match(transport_msg) || err.match(host_msg)) {
    log('Attempting to reconnect...');
    teardown_consumer(topic, id);
  }
};

const handle_consumer_close = (code, topic, work, options) => {
  let msg = `Consumer exited with code ${code}`;

  log(msg);
  if (! options.exit) {
    log(`Restarting consumer...`);
    setTimeout(consume, restart_consumer_interval, topic, work, options);
  }
  else throw new BrokerError(msg);

  return code;
};

const register_consumer = (consumer, topic, id) => {
  registered_consumers[topic] = registered_consumers[topic] || {};
  registered_consumers[topic][id] = consumer;

  return registered_consumers[topic];
};

const validate_arguments = (topic, work) => {
  if (topic instanceof Array && topic.length) {
    return { multi: true };
  }

  if (! topic || typeof topic != 'string') throw new BrokerError(
    'A topic argument is required!  It must be either an Array or String.'
  );

  if (! work || typeof work != 'function') throw new BrokerError(
    'A work argument is required!  It must be a function.'
  );

  return true;
};

const consume = (topic, work, options = {
  group: false, offset: 'end', exit: false
}) => {
  const { group = false, offset = 'end', exit = false } = options;
  if (
    validate_arguments(topic, work).multi && ! group
  ) return consume_multi_topics(topic, work, options);

  if (topic === '*') return consume(list_topics(), work, options);

  let refresh_interval = process.env.KAFKA_TOPIC_METADATA_REFRESH_INTERVAL_MS ||
    60000
  ;
  let consumer_type = group ?
    ['-G', group, (topic instanceof Array ? topic.join(' ') : topic)] :
    ['-C', '-t', topic]
  ;
  const id = uuid.v4();
  const consume_options = [
    '-b', brokers, '-o', offset, '-u', '-J',
    '-X', `topic.metadata.refresh.interval.ms=${refresh_interval}`,
  ].concat(consumer_type);

  log(`Consuming ${topic} at offset ${offset}`);
  log(`Kafkacat command: ${kafkacat} ${consume_options.join(' ')}`);
  const consumer = spawn(kafkacat, consume_options);

  consumer.stdout
    .pipe(split(JSON.parse))
    .on('data', (data) => handle_consumer_data(data, topic, id, work, exit))
    .on('error', (err) => { throw new BrokerError(err); });
  consumer.stderr
    .on('data', (data) => handle_consumer_error(data.toString(), topic, id));

  consumer
    .on('close', (code) => handle_consumer_close(code, topic, work, options));

  return register_consumer(consumer, topic, id);
};

const starve = (topic, id) => {
  if (! topic || typeof topic != 'string') throw new BrokerError(
    'A topic argument is required!  It must be either an Array or String.'
  );

  if (topic == '*') {
    Object.keys(registered_consumers).forEach((consumed_topic) => {
      starve(consumed_topic);
    });
    return registered_consumers;
  }

  if (
    ! registered_consumers[topic] ||
    (id && ! registered_consumers[topic][id])
  ) throw new BrokerError('No consumer to starve!');

  if (id) return teardown_consumer(topic, id);

  Object.keys(registered_consumers[topic]).forEach((consumer_id) => {
    teardown_consumer(topic, consumer_id);
  });

  delete registered_consumers[topic];

  return registered_consumers;
};

export { consume, starve, decorate_consumer };
