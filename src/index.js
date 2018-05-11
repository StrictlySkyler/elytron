import { spawn } from 'child_process';
import { log } from '../lib/logger';
import { produce, decorate_producer} from './produce';
import { consume, starve, decorate_consumer } from './consume';
import { run, brokers } from '../lib/run';

log(`Loading elytron using broker host string: ${brokers}`);
decorate_producer(spawn);
decorate_consumer(run, spawn);

process.on('SIGINT', () => {
  process.exit();
});
process.on('SIGTERM', () => {
  process.exit();
});
process.on('exit', () => {
  starve('*');
});

export { produce, consume, starve };

