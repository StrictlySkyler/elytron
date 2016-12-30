/* eslint no-new: 0 */

import assert from 'assert';
import Producer from '../source/producer';
import Kafka from './stubs/node-rdkafka';
import {
  set_received,
  produce_once,
  poll_producer,
  acknowledge_delivery_report,
  produce_at_least_once,
  produce
} from '../source/producer/methods';

afterEach(function () {
  Kafka.reset();
  set_received({});
});

describe('Producer', function () {
  it('should exist on import', function () {
    assert(Producer);
  });

  it('should attempt to connect on instantiation', function () {
    let test_config;
    let test_producer_settings;
    let test_topic_settings;
    let test_topics = [];
    test_config = test_producer_settings = test_topic_settings = {};

    new Producer(
      test_config,
      test_producer_settings,
      test_topic_settings,
      test_topics,
      Kafka
    );

    assert(Kafka.is_connected());
  });

  it('should return a function', function () {
    let test_config;
    let test_producer_settings;
    let test_topic_settings;
    let test_topics = [];
    test_config = test_producer_settings = test_topic_settings = {};

    let test_producer = new Producer(
      test_config,
      test_producer_settings,
      test_topic_settings,
      test_topics,
      Kafka
    );

    assert(typeof test_producer == 'function');
  });

  it('should register event handlers', function () {
    let test_config;
    let test_producer_settings;
    let test_topic_settings;
    let test_topics = [];
    test_config = test_producer_settings = test_topic_settings = {};

    new Producer(
      test_config,
      test_producer_settings,
      test_topic_settings,
      test_topics,
      Kafka
    );

    let handlers = Kafka.get_producer_handlers();

    assert(typeof handlers.ready == 'function');
    assert(typeof handlers['delivery-report'] == 'function');
    assert(typeof handlers.event == 'function');
    assert(typeof handlers.error == 'function');
  });

  it('should produce messages', function () {
    let test_config;
    let test_producer_settings;
    let test_topic_settings;
    let test_topics = [];
    test_config = test_producer_settings = test_topic_settings = {};
    let test_topic = 'test_topic';
    let test_partition = null;
    let test_message = 'test_message';

    let test_produce = new Producer(
      test_config,
      test_producer_settings,
      test_topic_settings,
      test_topics,
      Kafka
    );


    let messages = Kafka.get_produced_messages();
    assert(messages.length == 0);

    test_produce(test_topic, test_partition, test_message);

    messages = Kafka.get_produced_messages();
    assert(messages.length);

  });

  it('should error on invalid inputs', function () {
    let test_config;
    let test_producer_settings;
    let test_topic_settings;
    let test_topics = [];
    test_config = test_producer_settings = test_topic_settings = {};

    let throws_produce = new Producer(
      test_config,
      test_producer_settings,
      test_topic_settings,
      test_topics,
      Kafka
    );

    assert.throws(throws_produce);

  });

  it('#produce_once should return true on successful submission', function () {
    let test_topic = 'test_topic';
    let test_value = 'test_value';
    let test_buffer = Buffer.from('test_buffer');

    let produce_once_results = produce_once(
      test_topic, test_value, test_buffer
    );

    assert(produce_once_results === true);
  });

  it('#produce_once should return false on failed submission', function () {
    let produce_once_results = produce_once();

    assert(produce_once_results === false);
  });

  it(
    '#poll_producer should return the topic for which it is polling',
    function () {
      let test_topic = 'test';
      let test_message_package = {};

      let poll_producer_result = poll_producer(
        test_topic, test_message_package
      );

      assert.throws(poll_producer);
      assert.equal(test_topic, poll_producer_result);
    }
  );

  it(
    '#acknowledge_delivery_report should return an updated received hash',
    function () {
      let test_topic = 'test';
      let test_received_hash = {
        'test': {
          awaiting_report: true,
          timer: null
        }
      };

      set_received(test_received_hash);

      let updated_received_hash = acknowledge_delivery_report(test_topic);

      assert.deepEqual(updated_received_hash, {});
    }
  );

  it(
    '#produce_at_least_once should return the awaiting hash if new',
    function () {
      let test_topic = 'test';
      let test_message_package = { value: 'test_value' };

      let results = produce_at_least_once(test_topic, test_message_package);

      assert(typeof results == 'object');
      assert(results.awaiting_report === true);
    }
  );

  it(
    '#produce_at_least_once should return a timer if called more than once',
    function () {
      let test_topic = 'test';
      let test_message_package = { value: 'test_value' };

      let first_results = produce_at_least_once(
        test_topic, test_message_package
      );
      let second_results = produce_at_least_once(
        test_topic, test_message_package
      );

      assert.notDeepEqual(first_results, second_results);
      assert(typeof second_results._idleTimeout == 'number');
    }
  );

  it(
    '#produce_at_least_once should throw an error with invalid arguments',
    function () {
      let test_topic = 'test';
      let test_package = { value: 'test_value' };

      assert.throws(produce_at_least_once);
      assert.throws(() => { produce_at_least_once(test_topic); });
      assert.throws(() => { produce_at_least_once(null, test_package); });
    }
  );

  it(
    '#produce should return a status hash when passed a message argument',
    function () {
      let test_topic = 'test';
      let test_message = 'test_message';

      let test_status = produce(test_topic, test_message);

      assert(typeof test_status == 'object');
      assert(Object.keys(test_status).length > 1);
    }
  );

  it(
    '#produce should return a status hash when not passed a message argument',
    function () {
      let test_topic = 'test';

      let test_status = produce(test_topic);

      assert(typeof test_status == 'object');
      assert(Object.keys(test_status).length > 1);
    }
  );

  it(
    '#produce should package the message provided as an argument and return it',
    function () {
      let test_topic = 'test';
      let test_message = 'test_message';

      let test_status = produce(test_topic, test_message);

      assert(test_status.package);
      assert(test_status.package.value === test_message);
    }
  );

  it(
    '#produce should make a registration timestamp if not given a message',
    function () {
      let test_topic = 'test';

      let test_status = produce(test_topic);

      assert(test_status.package.value.registration);
      assert(typeof test_status.package.value.registration == 'number');
    }
  );

  it(
    '#produce should throw an error with invalid types of arguments',
    function () {
      let list_of_invalid_arguments_by_type = [
        Math.random(),
        [],
        {},
        '',
        /regex/,
        true,
        null,
        undefined,
        function () {},
        NaN,
        Symbol('symbol')
      ];

      list_of_invalid_arguments_by_type.forEach(function (invalid_topic) {
        assert.throws(() => { produce(invalid_topic); });
      });
    }
  );

  it(
    '#produce should report submitted as true or false',
    function () {
      let test_topic = 'test';

      let test_status = produce(test_topic);

      assert(typeof test_status.submitted == 'boolean');
    }
  );

  it(
    '#produce should report the results it receives',
    function () {
      let test_topic = 'test';

      let test_status = produce(test_topic);

      assert(test_status.results);
    }
  );

  it(
    '#produce should require a topic',
    function () {
      assert.throws(produce);
    }
  );
});

describe('Consumer', function () {
  it('should exist on import');
});

describe('Decorator', function () {
  it('should be pending');
});
