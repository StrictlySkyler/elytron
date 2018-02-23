import assert from 'assert';
import { produce } from '../src';
import { decorate_producer } from '../src/produce';
import { decorate_consumer } from '../src/consume';

let run = () => {
  return '';
};
let spawn = () => ({
  on: () => {},
  stdout: { on: () => {}, destroy: () => {} },
  stderr: { on: () => {}, destroy: () => {} },
});
const topic = 'news';
const message = 'hope';

describe('producing', () => {
  beforeEach(() => {
    decorate_producer(spawn);
    decorate_consumer(run, spawn);
  });

  it('should need a topic', () => {
    assert(produce(topic));
    assert.throws(produce);
  });

  it('should send a payload with a message', () => {
    const { payload } = produce(topic, message);
    assert(payload.id);
    assert(payload.timestamp);
    assert(payload.message);
    assert(! payload.response_topic);
  });

  it('should send a payload without a message', () => {
    const { payload } = produce(topic);
    assert(payload.id);
    assert(payload.timestamp);
    assert(! payload.message);
    assert(! payload.response_topic);
  });

  it('should consume a response', () => {
    const { payload } = produce(topic, message, run);
    assert(payload.response_topic);
  });
});
