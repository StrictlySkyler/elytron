import assert from 'assert';
import { consume, starve } from '../src';
import { decorate_producer } from '../src/produce';
import { decorate_consumer } from '../src/consume';

let run = () => {
  return '';
};
let spawn = () => ({
  on: () => spawn(),
  stdout: { pipe: () => spawn(), on: () => spawn(), destroy: () => spawn() },
  stderr: { pipe: () => spawn(), on: () => spawn(), destroy: () => spawn() },
  kill: () => {},
});
const topic = 'news';

describe('consuming', () => {
  beforeEach(() => {
    decorate_producer(run);
    decorate_consumer(run, spawn);
  });

  afterEach(() => {
    starve(topic);
  });

  it('needs at least one topic and some work', () => {
    assert(consume(topic, run));
    assert(consume([topic, topic], run));
    assert.throws(consume);
    assert.throws(() => consume(topic));
  });

  it('registers a consumer', () => {
    const registry = consume(topic, run);
    assert(Object.keys(registry).length == 1);
  });

});

describe('starving', () => {

  it('needs a consumed topic', () => {
    assert(() => starve(topic));
    assert.throws(starve);
    consume(topic, run);
    assert(starve(topic));
  });

  it('can stop a specific consumer', () => {
    const consumer_id = Object.keys(consume(topic, run))[0];
    const another_id = Object.keys(consume(topic, run))[1];
    const registry = starve(topic, consumer_id);
    assert(registry[another_id]);
    assert(! registry[consumer_id]);
  });
});
