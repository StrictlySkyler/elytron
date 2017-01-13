import { log } from '../../lib/logger';

let active_topics = [];
let handlers = {};
let produce;

let check_handlers = function (data, parsed_message) {
  if (parsed_message && parsed_message.registration) return false;

  let handlers_registered = [];
  log('Checking handlers for:', data.topic);

  if (handlers[data.topic]) {
    handlers_registered.push(data.topic);
  }

  if (handlers['*']) handlers_registered.push('*');

  if (handlers_registered.length) return handlers_registered;

  return false;
};

let handle_message = function (topics, message) {
  let results = {};

  topics.forEach(function (topic) {
    log('Calling handler for:', topic);
    results[topic] = handlers[topic](message);
  });

  return results;
};

let get_consumed_topics = function () {
  return active_topics;
};

let get_active_topics = function () {
  return active_topics;
};

let set_active_topics = function (topics) {
  active_topics = topics;
};

let reset_handlers = function () {
  handlers = {};
};

let get_topic_handlers = function () {
  return handlers;
};

let send_results = function (topic, results) {
  log('Sending results back to awaiting topic:', topic);

  let results_string = JSON.stringify(results);
  produce(parsed_message.awaiting_topic, results_string);
};

export {
  check_handlers,
  handle_message,
  get_consumed_topics,
  get_active_topics,
  set_active_topics,
  reset_handlers,
  get_topic_handlers,
  send_results
};
