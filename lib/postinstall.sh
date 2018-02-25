#!/bin/bash

log () {
  echo `date`": $1"
}

if [ "$CI" == "true" ]; then
  log "Running in CI, not checking for kafkacat."
  exit 0;
fi

log 'Checking for kafkacat...'
kafkacat=`which kafkacat`

if [ -z "$kafkacat" ]; then
  log "No kafkacat found!"
  log "Please make sure the kafkacat library is available in the PATH."
  log "See here for details: https://github.com/edenhill/kafkacat"
  exit 1
fi

log "Found kafkacat at: $kafkacat"
