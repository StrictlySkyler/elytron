import { decorate, produce, validate_arguments } from '../../source/methods';
import assert from 'assert';

describe('Decorator', function () {
  it('#decorate should return the assigned decorations', function () {
    let test_function = function () {};
    let results = decorate(test_function);

    assert(test_function, results);
  });

  it('#produce should return the produced results', function () {
    let test_function = function (topic, message) {
      return {
        topic: topic,
        message: message
      };
    };
    let test_config = {};
    let test_args = { topic: 'test', message: 'test message' };

    decorate(test_config, test_function);

    let results = produce(test_args);

    assert.deepEqual(results, test_args);
  });

  it('#produce should throw with invalid arguments', function () {
    assert.throws(produce);
  });

  it('#validate_arguments should validate proper arguments', function () {
    let test_topic = 'test';
    let test_message = {};

    assert(validate_arguments(test_topic, test_message));
    assert.throws(validate_arguments);
    assert.throws(function () { validate_arguments(null, test_message); });
    assert.throws(function () { validate_arguments(test_topic, null); });
  });
});

