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

import { clickButton, setLabeledInput } from '../../util/playwright';

/**
 * Where the data loader reads the data from.
 */
export type DataConnector =
  | { readonly type: 'local'; readonly baseDirectory: string; readonly fileFilter: string }
  | { readonly type: 'reindex'; readonly datasourceName: string; readonly interval: string };

/**
 * The title of the connector's card on the data loader's start page.
 */
export function connectorCardTitle(connector: DataConnector): string {
  switch (connector.type) {
    case 'local':
      return 'Local disk';
    case 'reindex':
      return 'Reindex from Druid';
  }
}

/**
 * Whether the data needs parsing, so goes through the Parse data and Parse time steps (data from Druid doesn't).
 */
export function connectorNeedsParse(connector: DataConnector): boolean {
  return connector.type !== 'reindex';
}

/**
 * Fills in the Connect step and applies it.
 */
export async function connect(page: Page, connector: DataConnector): Promise<void> {
  switch (connector.type) {
    case 'local':
      await setLabeledInput(page, 'Base directory', connector.baseDirectory);
      await setLabeledInput(page, 'File filter', connector.fileFilter);
      break;

    case 'reindex':
      await setLabeledInput(page, 'Datasource', connector.datasourceName);
      await setLabeledInput(page, 'Interval', connector.interval);
      break;
  }
  await clickButton(page, 'Apply');
}
