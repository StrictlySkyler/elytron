# elytron
A interface for Kafka in NodeJS with sensible defaults.

Compatible with Kafka 0.8.x.x and higher.

Tested with Kafka 0.10.1.0.

Elytron relies on Node 4+, and is compatible with Meteor.

## Features
Elytron provides an interface for interacting with a Kafka cluster via NodeJS and Meteor in an idiomatic, straightforward manner.  It abstracts away many of the underlying interactions provided by node-rdkafka to provide a clean, simple interface, while still exposing the underlying nitty-gritty functionality if needed.

Specifically, elytron provides the following when interacting with Kafka:

- Straightforward API
- Automatic topic creation
- Mapping of topic consumption events to user-prodvided callbacks
- Good Enough™ defaults for the Kafka Client
- Overriding of Kafka Client defaults with Environment Variables
- Automatic retry until hard failure

## Usage

Elytron can be installed by running `npm i --save elytron` in the terminal.

Elytron's API exposes two things for developer use: the `produce` function and the `consumer` object.  They can be accessed like so:

```javascript
import { produce, consumer } from 'elytron';
```

Alternately, without ES6:
```javascript
var roach = require('elytron');
var produce = roach.produce;
var consumer = roach.consumer;
```

### Producing Messages
At a basic level, a message can be produced and sent to Kafka on any given topic merely by calling the `produce` function and passing it the name of a topic as a string.  Example:

```javascript
produce('an_interesting_topic');
```

Every message produced by elytron includes a timestamp and a unique identifier with the original message.  If no message is provided, as in the example above, a "registration" message is created; its value is set to the timestamp of when it is called.

Usually a message is included when producing to a Kafka topic, even if elytron doesn't explicitly require one to be provided.  A message can be included like so:

```javascript
produce('an_interesting_topic', a_relevant_message);
```

The message provided must be a JSON-serializable object.

`produce` returns a hash containing the status of the produced message.  An example hash might look like:
```javascript
let bar = { bar: 'bar' };
let status = produce('foo', bar);
console.log(status);
//{
//  package: {
//    timestamp: 1484272028549,
//    id: '1befd1ad-351e-47fe-bb1a-eb5019cbfbd9',
//    value: {
//      // If no message is provided, this would be:
//      // registration: 1484272028549
//      bar: 'bar'
//    }
//  },
//  results: {
//    awaiting_report: true
//  },
//  // This will be false if something went wrong, like an unreachable Kafka.
//  submitted: true
//}
```

The `produce` function will retry producing to Kafka until it succeeds; if it fails to reach Kafka and cannot recover (such as due to a network outage), it will throw an error.

#### Callbacks & Responses

The `produce` method accepts an optional callback as a third argument:
```javascript
function some_work_to_do (message) {
  // Work based on the response message happens here
}

produce('an_interesting_topic', a_relevant_message, some_work_to_do);
```

If a callback is provided, elytron will create a "private" topic using a UUID hash, automatically create a consumer for it, and include the name of the `awaiting_topic` in its initial message payload.  This allows for a consumer listening on the initial topic to provide a response message, which is in turn passed to the `some_work_to_do` callback.  In this way, elytron can be leveraged for a "request/response" model, passing everything through Kafka.


#### The Producer Object
Generally speaking, the `produce` method should cover all the needs for producing messages.  However, should you need to access the underlying client, such as to assign event listeners, elytron exposes the underlying node-rdkafka client for producers.

It can be obtained like so:
```javascript
import { get_producer_client } from 'elytron';

let producer_client = get_producer_client();

producer_client.on(...);
```

The producer client merely exposes the underlying node-rdkafka API; see [node-rdkafka](https://blizzard.github.io/node-rdkafka/current/Producer.html) for details.

### Consumer
There are multiple ways to consume topics with the `consumer` object.

#### `.topic`
Perhaps the easiest and most idiomatic way is to use the `topic` method to assign a callback to be triggered any time a message is consumed from a topic, like so:
```javascript
consumer.topic('foo', function () { /* Do work */ });
```

#### `.topics`
Alternatively, you can use the `topics` (plural) method to pass a hash containing topics as keys and the function to execute as values, like so:
```javascript
consumer.topics({
  'foo': function () { /* Do work */ },
  'bar': function () { /* Some other work */ }
});
```

#### `*` (wildcard)
Elytron also supports a "wildcard topic", which will execute the function assigned to it any time *any* message is consumed from *any* topic:
```javascript
consumer.topics({
  '*': function () {},   // Executes for foo, bar, and any others
  'foo': function () {}, // Only executes when a message is consumed from "foo"
  'bar': function () {}  // Only executes when a message is consumed from "bar"
});
```

#### `.consume`
Topics can be consumed without passing a function to handle them via the `consume` method, like so:
```javascript
consumer.consume(['baz', 'qux']);
```

Wildcard functions will execute for topics consumed in this way.  If no function is assigned to these topics, or as a wildcard, no work will be done, but the message will still be consumed.

#### `.starve`
Elytron can stop consuming from topics via the `starve` method, like so:
```javascript
consumer.starve();
```

Function handlers assigned to specific topics will be preserved by default, allowing for resuming consumption of messages at a later time.  To remove these handlers, pass a truthy value to `starve`, and it will remove all of them:
```javascript
consumer.starve(true); // All topics and handlers will be removed.
```

#### `.on` & `.off`
Both the `on` and `off` methods are exposed from the underlying client and can register event listeners normally, just as node-rdkafka (and EventEmitter, in turn) themselves do.  See their documentation for details on how to use these methods.

## Options & Defaults
Each of elytron's settings can be overridden by setting an environment variable with the desired value.  The names of these values, along with their default settings, can be found by viewing elytron's [config file](https://github.com/StrictlySkyler/elytron/blob/master/source/config.js).

## Tests

To run tests for elytron, within the repo execute either `npm test` to run the suite, or `npm run watch` to execute the suite and watch for changes.
