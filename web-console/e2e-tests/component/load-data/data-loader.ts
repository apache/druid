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

import { clickButton, openView, setLabeledInput, setLabeledTextarea } from '../../util/playwright';
import { showStep } from '../../util/steps';

import type { DataConnector } from './data-connector';
import { connect, connectorCardTitle, connectorNeedsParse } from './data-connector';
import type { PartitionsSpec } from './partitions-spec';
import { applyPartitionsSpec } from './partitions-spec';

/**
 * What to set at each step of the (classic) data loader.
 */
export interface DataLoaderConfig {
  // Connect
  readonly connector: DataConnector;
  /** Checks the raw lines of the preview */
  readonly validateConnect: (previewLines: string[]) => void;
  // Parse time (when the connector's data needs parsing)
  readonly timestampExpression?: string;
  // Configure schema
  readonly rollup: boolean;
  // Partition
  readonly segmentGranularity: 'hour' | 'day' | 'month' | 'year';
  readonly timeIntervals?: string;
  readonly partitionsSpec?: PartitionsSpec;
  // Publish
  readonly datasourceName: string;
}

/**
 * Goes through each step of the data loader and submits the task.
 */
export async function loadData(page: Page, config: DataLoaderConfig): Promise<void> {
  const nextBar = page.locator('.next-bar');
  const clickNext = (step: string) => clickButton(nextBar, `Next: ${step}`);

  await openView(page, 'data-loader');
  await page
    .locator('.bp6-card')
    .filter({ has: page.locator('p', { hasText: connectorCardTitle(config.connector) }) })
    .click();
  await showStep(page, `Data loader: ${connectorCardTitle(config.connector)} picked`);
  await clickButton(page, 'Connect data');

  // Connect
  await connect(page, config.connector);
  const rawLines = page.locator('.raw-lines .raw-line');
  await rawLines.first().waitFor();
  config.validateConnect(await rawLines.allTextContents());
  await showStep(page, 'Data loader: Connect, with a preview of the data');

  if (connectorNeedsParse(config.connector)) {
    await clickNext('Parse data');

    // Parse data
    await page.locator('.parse-data-table').waitFor();
    await showStep(page, 'Data loader: Parse data');
    await clickNext('Parse time');

    // Parse time
    await page.locator('.parse-time-table').waitFor();
    if (config.timestampExpression) {
      await clickButton(page, 'Expression');
      await setLabeledInput(page, 'Expression', config.timestampExpression);
      await clickButton(page, 'Apply');
    }
    await showStep(page, 'Data loader: Parse time');
  }
  await clickNext('Transform');

  // Transform
  await page.locator('.transform-table').waitFor();
  await showStep(page, 'Data loader: Transform');
  await clickNext('Filter');

  // Filter
  await page.locator('.filter-table').waitFor();
  await showStep(page, 'Data loader: Filter');
  await clickNext('Configure schema');

  // Configure schema
  await page.locator('.schema-table').waitFor();
  await setRollup(page, config.rollup);
  await showStep(page, 'Data loader: Configure schema');
  await clickNext('Partition');

  // Partition
  await page.locator('.load-data-view.partition').waitFor();
  await setLabeledInput(page, 'Segment granularity', config.segmentGranularity);
  if (config.timeIntervals) {
    await setLabeledTextarea(page, 'Time intervals', config.timeIntervals);
  }
  if (config.partitionsSpec) {
    await applyPartitionsSpec(page, config.partitionsSpec);
  }
  await showStep(page, 'Data loader: Partition');
  await clickNext('Tune');

  // Tune
  await page.locator('.load-data-view.tuning').waitFor();
  await showStep(page, 'Data loader: Tune');
  await clickNext('Publish');

  // Publish
  await page.locator('.load-data-view.publish').waitFor();
  await setLabeledInput(page, 'Datasource name', config.datasourceName);
  await showStep(page, 'Data loader: Publish');
  await clickNext('Edit spec');

  // Edit spec
  await page.locator('.load-data-view.spec').waitFor();
  await showStep(page, 'Data loader: Edit spec');
  await clickButton(nextBar, 'Submit task');
}

async function setRollup(page: Page, rollup: boolean): Promise<void> {
  const rollupSwitch = page.getByLabel('Rollup', { exact: true });
  if ((await rollupSwitch.isChecked()) === rollup) return;

  // The switch's input is visually hidden, so click its label (which asks for confirmation)
  await page.locator('label', { has: rollupSwitch }).click();
  await clickButton(page.locator('.bp6-alert'), `Yes - ${rollup ? 'enable' : 'disable'} rollup`);
  await page.locator('.recipe-toaster').getByRole('button').click();
}
