import os from 'os';
import { spawnSync } from 'child_process';

let run = (command, flags) => {
  const encoding = 'utf8';
  let results = spawnSync(command, flags, { encoding })
    .stdout
    .replace('\n', '')
  ;

  return results;
};

const kafkacat = run('which', ['kafkacat']);
const tmp = os.tmpdir();
const brokers = process.env.KAFKA_BROKERS ||
  'localhost'
;

export { run, kafkacat, tmp, brokers };
