import uuid from 'uuid';
import { kafkacat, brokers } from '../../lib/run';
import { log, error } from '../../lib/logger';
import { BrokerError } from '../../lib/error';
import { produce } from '../produce';

let registered_consumers = {};
let run;
let spawn;

const delimiter = ':msg:';

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
  let parsed = data.split(delimiter).reverse();
  // Remove any empty string after the delimiter
  if (parsed[0] === '') parsed.shift();

  let i = parsed.length;

  while (i--) {
    try { // Attempt to parse our most recent chunk
      let deserialized = JSON.parse(parsed[i]);
      let payload;

      log(`Consumed data from ${topic}: ${deserialized.payload}`);
      let results = work(deserialized.payload);

      try { payload = JSON.parse(deserialized.payload); }
      catch (e) { payload = deserialized.payload; }

      if (payload.response_topic) produce(payload.response_topic, results);
    }
    catch (err) { // Incomplete chunk from string, save for next event
      error(`Unable to parse data chunk: ${parsed[i]}`, err);
      break;
    }

    parsed.splice(i, 1);
  }

  if (exit) teardown_consumer(topic, id);

  return parsed.join('');
};

const handle_consumer_error = (err) => error(
  `Received error from consumer: ${err}`
);

const handle_consumer_close = (code) => {
  log(`Consumer exited with code ${code}`);
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
  group: false, offset: 'beginning', exit: false
}) => {
  const { group = false, offset = 'beginning', exit = false } = options;
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
    '-b', brokers, '-D', delimiter, '-o', offset, '-u', '-J',
    '-X', `topic.metadata.refresh.interval.ms=${refresh_interval}`,
  ].concat(consumer_type);

  let stdout = '';
  let stale_cache_timer;

  log(`Consuming ${topic} at offset ${offset}`);
  const consumer = spawn(kafkacat, consume_options);

  consumer.stdout.on('data', (data) => {
    clearTimeout(stale_cache_timer);
    stdout += data.toString();
    stdout = handle_consumer_data(stdout, topic, id, work, exit);
    stale_cache_timer = setTimeout(
      () => stdout = '',
      process.env.ELYTRON_STALE_CACHE_TIMER || refresh_interval
    );
  });
  consumer.stderr.on('data', (data) => {
    handle_consumer_error(data.toString());
  });

  consumer.on('close', handle_consumer_close);

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
