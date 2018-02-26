# elytron
[![Build Status](https://travis-ci.org/StrictlySkyler/elytron.svg?branch=master)](https://travis-ci.org/StrictlySkyler/elytron)

_n._

1. ~~The hardened shell of a beetle.~~<sup>[1](https://en.wikipedia.org/wiki/The_Metamorphosis)</sup>
2. A handy interface for Kafka in Node.

Compatible with Kafka 0.8.x.x and higher.

Elytron relies on the [`kafkacat`](https://github.com/edenhill/kafkacat) C library.  See kafkacat for instructions on how to install it.

## Usage

Elytron can be installed by running `npm i -s elytron` in the terminal.

Elytron's API exposes three things:

```javascript
import { produce, consume, starve } from 'elytron';
```

### Producing Messages
A message can be produced and sent to Kafka on a topic by calling the `produce` function and passing it the name of a topic as a string.  Example:

```javascript
produce('an_interesting_topic');
```

Every message produced by elytron includes a timestamp and a unique identifier with the original message.  If no message is provided, as in the example above, a "registration" message is created; its value is set to the timestamp of when it is called.

A message can be included like so:

```javascript
produce('an_interesting_topic', a_relevant_message);
```

The message provided must be JSON-serializable.

`produce` returns a hash containing the status of the produced message.  An example hash might look like:
```javascript
let message = { presses: 'stop' };
let status = produce('news', message);
console.log(status);
//{
//  payload: {
//    timestamp: 1484272028549,
//    id: '1befd1ad-351e-47fe-bb1a-eb5019cbfbd9',
//    value: {
//      // If no message is provided, this would be:
//      // registration: 1484272028549
//      presses: 'stop'
//    }
//  }
//}
```

#### Callbacks & Responses

The `produce` method accepts an optional callback as a third argument:
```javascript
function work (response) {
  // Work based on the response message happens here
}

produce('an_interesting_topic', a_relevant_message, some_work_to_do);
//{
//  payload: {
//    timestamp: 1484272028549,
//    id: '1befd1ad-351e-47fe-bb1a-eb5019cbfbd9',
//    value: {
//      bar: 'bar'
//    },
//    response_topic: "response.an_interesting_topic.1befd1ad-351e-47fe-bb1a-eb5019cbfbd9"
//  }
//}
```

If a callback is provided, elytron will create a "private" topic using a UUID, automatically create a consumer for it, and include the name of the `response_topic` in its initial message payload.  This allows for a consumer listening on the initial topic to provide a response message, which is in turn passed to the `some_work_to_do` callback as a response.

### Consuming Messages

There are multiple ways to consume topics with the `consume` function.

```javascript
// Consume a single topic, do some work for each message
consume('news', (msg) => { /* Do work */ });

// Consume multiple topics, doing work for messages on any of them
consume(['media', 'entertainment'], (msg) => {
  // Returned values from a consumer's callback get produced on the
  // response_topic, if it's present
  return msg.split('').reverse().join('');
});

// Consume as a High-Level Consumer, part of a balanced Consumer Group
consume(topic, work, group);

// Consume a topic starting at offset 5 (e.g. consume from the 6th on), and
// continue through all subsequent messages
consume(topic, work, false, 5);

// Same as above, but exit when the last message has been consumed
consume(topic, work, false, 5, true);

// Consume on all available topics
consume('*', (msg) => {});
```

Elytron can stop consuming from topics via the `starve` function, like so:

```javascript
// Stop all consumers of sugar
starve('sugar');

// Stop a particular consumer from television
starve('television', "1befd1ad-351e-47fe-bb1a-eb5019cbfbd9");
```

## Tests

To run tests for elytron, within the repo execute either `npm test` to run the suite, or `npm run watch` to execute the suite and watch for changes.
