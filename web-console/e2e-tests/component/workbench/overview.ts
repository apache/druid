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

import { clickButton, openView, setQueryInput } from '../../util/playwright';
import { extractTable } from '../../util/table';

/**
 * Represents the workbench tab.
 */
export class WorkbenchOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async runQuery(query: string): Promise<string[][]> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);
    await clickButton(this.page, 'Run');

    const results = this.page.locator('.result-table-pane');
    const capacityAlert = this.page.locator('.alert-dialog').filter({
      hasText: 'The cluster does not currently have enough available task slots',
    });
    const timeout = 4 * 60 * 1000;
    await results.or(capacityAlert).waitFor({ timeout });
    if (await capacityAlert.isVisible()) {
      await clickButton(capacityAlert, 'Run it anyway');
      await results.waitFor({ timeout });
    }

    return await extractTable(results.locator('.ct-tr-group'), '.ct-td');
  }
}
