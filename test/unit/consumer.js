/* eslint no-new: 0 */

import assert from 'assert';
import Kafka from '../stubs/node-rdkafka';
import {
  Consumer,
  check_handlers,
  handle_message,
  get_consumed_topics
} from '../../source/consumer';
import {
  get_topic_handlers
} from '../../source/consumer/methods';

describe('Consumer', function () {
  afterEach(function () {
    Kafka.reset();
    new Consumer(null, null, null, Kafka).starve(true);
  });

  it('should exist on import', function () {
    assert(Consumer);
  });

  it('should attempt to connect on instantiation', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = [];

    new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );

    assert(Kafka.is_connected());
  });

  it('should consume topics passed on instantiation', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['foo', 'bar'];

    new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );
    Kafka.ready();

    let topics_consumed = get_consumed_topics();

    assert.deepStrictEqual(topics_consumed.sort(), test_topics.sort());
  });

  it('should return a consumer', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['foo', 'bar'];

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );

    assert(consumer instanceof Consumer);
  });

  it('should register event handlers on instantiation', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['foo', 'bar'];

    new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );

    let handlers = Kafka.get_consumer_handlers();

    assert(typeof handlers.ready == 'function');
    assert(typeof handlers.data == 'function');
    assert(typeof handlers.error == 'function');
    assert(typeof handlers['event.log'] == 'function');
  });

  it(
    'should trigger callbacks with message data when published to a topic',
    function () {
      let test_consumer_settings;
      let test_topic_settings;
      let test_topics = ['test'];
      let count = 0;
      let increment_count = function () { ++count; };

      let consumer = new Consumer(
        test_consumer_settings, test_topic_settings, test_topics, Kafka
      );

      let handlers = Kafka.get_consumer_handlers();
      let test_message = {
        topic: 'test',
        value: Buffer.from(JSON.stringify(''))
      };

      consumer.topics({
        '*': increment_count,
        'test': increment_count
      });
      handlers.data(test_message);

      assert.equal(count, 2);
    }
  );

  it(
    'should not trigger callbacks when none are registered to a topic',
    function () {
      let test_consumer_settings;
      let test_topic_settings;
      let test_topics = ['test'];
      let count = 0;
      let increment_count = function () { ++count; };

      let consumer = new Consumer(
        test_consumer_settings, test_topic_settings, test_topics, Kafka
      );

      let handlers = Kafka.get_consumer_handlers();
      let test_message = {
        topic: 'should_fire',
        value: Buffer.from(JSON.stringify(''))
      };

      consumer.topics({
        '*': increment_count,
        'should_fire': increment_count,
        'not_firing': increment_count
      });
      handlers.data(test_message);

      assert.equal(count, 2);
    }
  );

  it(
    'should trigger a callback registered for the "*" topic from any topic',
    function () {
      let test_consumer_settings;
      let test_topic_settings;
      let test_topics = ['test'];
      let count = 0;
      let increment_count = function () { ++count; };

      let consumer = new Consumer(
        test_consumer_settings, test_topic_settings, test_topics, Kafka
      );

      let handlers = Kafka.get_consumer_handlers();
      let test_message = {
        topic: 'should_fire',
        value: Buffer.from(JSON.stringify(''))
      };
      let another_test_message = {
        topic: 'also_fire',
        value: Buffer.from(JSON.stringify(''))
      };
      let wildcard_test_message = {
        topic: 'wildcard',
        value: Buffer.from(JSON.stringify(''))
      };

      consumer.topics({
        '*': increment_count,
        'should_fire': increment_count,
        'also_firing': increment_count
      });
      handlers.data(test_message);
      handlers.data(another_test_message);
      handlers.data(wildcard_test_message);

      assert.equal(count, 4);
    }
  );

  it('should check for handlers on a topic', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['test'];
    let test_callback = function () {}; //no-op
    let test_data_packet = {
      topic: 'test',
      value: {}
    };
    let registration_data_packet = {
      topic: 'test',
      value: {
        registration: true
      }
    };
    let unhandled_topic = {
      topic: 'unhandled',
      value: {}
    };

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );

    consumer.topic('test', test_callback);

    assert(check_handlers(test_data_packet));
    assert(! check_handlers(
      registration_data_packet,
      registration_data_packet.value
    ));
    assert(! check_handlers(unhandled_topic));
  });

  it('should handle messages passed to topic handlers', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['test'];
    let count = 0;
    let increment_count = function () { return ++count; };

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );
    let test_message = {};

    consumer.topics({
      '*': increment_count,
      'test': increment_count
    });

    handle_message(['*', 'test'], test_message);

    assert.equal(count, 2);
  });

  it('should provide a list of currently consumed topics', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['test', 'also_test'];

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );
    Kafka.ready();

    assert.equal(consumer.topics().length, 2);
  });

  it('should unsubscribe from topics', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['test', 'also_test'];

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );
    Kafka.ready();

    assert.equal(consumer.topics().length, 2);

    consumer.starve();

    assert.equal(consumer.topics().length, 0);
  });

  it('should consume no topics when none are passed', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = [];

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );
    Kafka.ready();

    assert.equal(consumer.topics().length, 0);
  });

  it('should be able to consume multiple topics at once', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topics = ['test', 'also_test'];
    let more_test_topics = ['further_test', 'still_a_test'];

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, test_topics, Kafka
    );
    Kafka.ready();
    consumer.consume(more_test_topics);

    assert.equal(consumer.topics().length, 4);
  });

  it('should be able to consume single topics at a time', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';
    let another_test_topic = 'also_test';
    let still_another_test_topic = 'further_test';
    let yet_another_test_topic = 'more_test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );
    Kafka.ready();
    consumer.consume([another_test_topic]);
    consumer.topic(still_another_test_topic, function irrelevant () {});
    consumer.topics({ [yet_another_test_topic]: function irrelevant () {} });

    assert.equal(consumer.topics().length, 4);
  });

  it('should error on invalid inputs for instantiation', function () {
    assert.throws(function () { new Consumer(); });
    assert.throws(function () { new Consumer(null, null, null, true); });
    assert.throws(function () { new Consumer(null, null, null, false); });
    assert.throws(function () { new Consumer(null, null, null, ''); });
    assert.throws(function () { new Consumer(null, null, null, 0); });
    assert.throws(function () { new Consumer(null, null, null, 1); });
    assert.throws(function () { new Consumer(null, null, null, 'foo'); });
    assert.throws(function () { new Consumer(null, null, null, {}); });
    assert.throws(function () { new Consumer(null, null, null, []); });
    assert.throws(function () { new Consumer(null, null, null, NaN); });
  });

  it('#consume should return itself', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );
    let returned_value = consumer.consume();

    assert(returned_value instanceof Consumer);
    assert(returned_value == consumer);
  });

  it('#consume should error with invalid arguments', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    assert.throws(function () { consumer.consume(true); });
  });

  it('#starve should return itself', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );
    let returned_value = consumer.starve();

    assert(returned_value instanceof Consumer);
    assert(returned_value == consumer);
  });

  it('should allow registering handlers for events', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';
    let test_function = function () {};

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    consumer.on('test', test_function);

    let handlers = Kafka.get_consumer_handlers();

    assert.equal(handlers.test, test_function);
  });

  it('should allow removing handlers for events', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';
    let test_function = function () {};

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    consumer.off('test', test_function);

    let handlers = Kafka.get_consumer_handlers();

    assert(! handlers.test);
  });

  it('#on should return itself', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';
    let test_function = function () {};

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    let results = consumer.on('test', test_function);

    assert.equal(results, consumer);
    assert(consumer instanceof Consumer);
  });

  it('#on should error with invalid arguments', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    assert.throws(consumer.on);
  });

  it('#off should return itself', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';
    let test_function = function () {};

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    let results = consumer.off('test', test_function);

    assert.equal(results, consumer);
    assert(consumer instanceof Consumer);
  });

  it('#off should error with invalid inputs', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    assert.throws(consumer.on);
  });

  it(
    '#topic should return the same type of thing #consume returns',
    function () {

      let test_consumer_settings;
      let test_topic_settings;
      let test_topic = 'test';

      let consumer = new Consumer(
        test_consumer_settings, test_topic_settings, [test_topic], Kafka
      );

      let consume_results = consumer.consume([test_topic]);
      let topic_results = consumer.topic(test_topic, function () {});

      assert.equal(consume_results, topic_results);
    });

  it('#topic should error with invalid arguments', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    assert.throws(consumer.topic);
  });

  it(
    '#topics should return the same type of thing #consume returns',
    function () {

      let test_consumer_settings;
      let test_topic_settings;
      let test_topic = 'test';

      let consumer = new Consumer(
        test_consumer_settings, test_topic_settings, [test_topic], Kafka
      );

      let consume_results = consumer.consume([test_topic]);
      let topics_results = consumer.topics({ [test_topic]: function () {} });

      assert.equal(consume_results, topics_results);
    }
  );

  it('#topics should error with invalid arguments', function () {
    let test_consumer_settings;
    let test_topic_settings;
    let test_topic = 'test';

    let consumer = new Consumer(
      test_consumer_settings, test_topic_settings, [test_topic], Kafka
    );

    assert.throws(function () { consumer.topics([]); });
  });
});

