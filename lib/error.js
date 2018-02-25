class BrokerError extends Error {
  constructor (...args) {
    super(...args);
    Error.captureStackTrace(this, BrokerError);
  }
}

export { BrokerError };
