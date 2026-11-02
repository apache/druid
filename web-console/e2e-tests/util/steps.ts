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

import type { Page } from '@playwright/test';
import { test } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

const SHOW_STEPS = process.env['E2E_SHOW_STEPS'] === 'true';
const STEPS_DIR = path.join(__dirname, '..', 'steps');

// The number of the last step shown, by spec (the counting starts over in each run)
const lastStepBySpec = new Map<string, number>();

/**
 * Marks a step of the test worth seeing: with E2E_SHOW_STEPS=true, it saves a screenshot of the page to
 * e2e-tests/steps/<spec>-<number>.png (like s3-ingestion-001.png), so that the steps a test takes can be seen at a
 * glance. Otherwise it does nothing.
 * @param title what the page shows at this step, which is how the step is named in the trace and the report
 */
export async function showStep(page: Page, title: string): Promise<void> {
  if (!SHOW_STEPS) return;

  const spec = path.basename(test.info().file).replace(/\.spec\.ts$/, '');
  const step = (lastStepBySpec.get(spec) ?? 0) + 1;
  if (step === 1) deleteSteps(spec); // of an earlier run, which could have had more steps
  lastStepBySpec.set(spec, step);

  await test.step(`Step ${step}: ${title}`, async () => {
    await page.screenshot({
      path: path.join(STEPS_DIR, `${spec}-${String(step).padStart(3, '0')}.png`),
      animations: 'disabled', // finishes the transitions (like a dialog fading in) rather than catch them midway
    });
  });
}

function deleteSteps(spec: string): void {
  if (!fs.existsSync(STEPS_DIR)) return;
  for (const file of fs.readdirSync(STEPS_DIR)) {
    if (file.startsWith(`${spec}-`) && /^-\d+\.png$/.test(file.slice(spec.length))) {
      fs.unlinkSync(path.join(STEPS_DIR, file));
    }
  }
}
