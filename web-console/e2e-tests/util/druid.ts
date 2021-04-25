/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { execSync } from 'child_process';
import path from 'path';

const UNIFIED_CONSOLE_PORT = process.env['DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT'] || '8888';
export const UNIFIED_CONSOLE_URL = `http://localhost:${UNIFIED_CONSOLE_PORT}/unified-console.html`;
export const COORDINATOR_URL = 'http://localhost:8081';

const UTIL_DIR = __dirname;
const E2E_TEST_DIR = path.dirname(UTIL_DIR);
const WEB_CONSOLE_DIR = path.dirname(E2E_TEST_DIR);
const DRUID_DIR = path.dirname(WEB_CONSOLE_DIR);
export const DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR = path.join(
  DRUID_DIR,
  'examples',
  'quickstart',
  'tutorial',
);

export function runIndexTask(ingestionSpecPath: string, sedCommands: Array<string>) {
  const postIndexTask = path.join(DRUID_DIR, 'examples', 'bin', 'post-index-task');
  const sedCommandsString = sedCommands.map(sedCommand => `-e '${sedCommand}'`).join(' ');
  execSync(
    `${postIndexTask} \
       --file <(sed ${sedCommandsString} ${ingestionSpecPath}) \
       --url ${COORDINATOR_URL}`,
    {
      shell: 'bash',
      timeout: 3 * 60 * 1000,
    },
  );
}
