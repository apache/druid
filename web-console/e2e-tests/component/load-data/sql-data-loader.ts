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

import { clickButton, getLabeledInput, openView, setLabeledInput } from '../../util/playwright';
import { showStep } from '../../util/steps';
import { waitForTaskSlots } from '../workbench/overview';

import type { DataConnector } from './data-connector';
import { connectorCardTitle, fillConnector } from './data-connector';

/**
 * What to set at each step of the SQL data loader ("Load data" > "Batch - SQL"), which ingests with an MSQ task.
 */
export interface SqlDataLoaderConfig {
  readonly connector: DataConnector;
  /** Checks the input format that the data loader picked and the columns it parsed */
  readonly validateParseData?: (parsed: { inputFormat: string; columns: string[] }) => void;
  /** The new datasource to create */
  readonly datasourceName: string;
}

/**
 * Goes through each step of the SQL data loader, starts the ingestion and waits for it to finish. Throws if it fails.
 */
export async function loadDataWithSql(page: Page, config: SqlDataLoaderConfig): Promise<void> {
  await openView(page, 'sql-data-loader');

  // Select input type
  await page
    .locator('.input-source-step .bp6-card')
    .filter({ has: page.locator('p', { hasText: connectorCardTitle(config.connector) }) })
    .click();
  await fillConnector(page, config.connector);
  await showStep(page, `SQL data loader: ${connectorCardTitle(config.connector)}`);
  await clickButton(page, 'Connect data');

  // Parse
  const parseDataTable = page.locator('.input-format-step .parse-data-table');
  await parseDataTable.locator('.column-name').first().waitFor();
  if (config.validateParseData) {
    config.validateParseData({
      inputFormat: await getLabeledInput(page, 'Input format'),
      columns: await parseDataTable.locator('.column-name').allTextContents(),
    });
  }
  await showStep(page, 'SQL data loader: Parse');
  await clickButton(page.locator('.input-format-step .prev-next-bar'), 'Next');

  // Schema, with the destination (a new datasource)
  const schemaStep = page.locator('.schema-step');
  await schemaStep.locator('.destination-button').click();
  const destinationDialog = page.locator('.destination-dialog');
  await clickButton(destinationDialog, 'New table');
  await setLabeledInput(destinationDialog, 'New table name', config.datasourceName);
  await showStep(page, 'SQL data loader: Destination');
  await clickButton(destinationDialog, 'Save');
  await destinationDialog.waitFor({ state: 'detached' });
  await showStep(page, 'SQL data loader: Schema');
  await clickButton(schemaStep.locator('.prev-next-bar'), 'Start loading data');

  // Ingestion progress
  const progressDialog = page.locator('.ingestion-progress-dialog');
  const done = progressDialog.getByText('Done loading data into');
  const error = progressDialog.getByText('Error ingesting data');
  await waitForTaskSlots(page, done.or(error));
  await showStep(page, 'SQL data loader: Ingestion progress, done');
  if (await error.isVisible()) {
    throw new Error(`SQL data loader failed: ${await error.innerText()}`);
  }
}
