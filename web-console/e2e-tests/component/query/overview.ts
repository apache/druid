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
 * Represents query overview tab.
 */
export class QueryOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async runQuery(query: string): Promise<string[][]> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);
    await clickButton(this.page, 'Run');

    const results = this.page.locator('.result-table-pane');
    const error = this.page.locator('.execution-error-pane');
    await results.or(error).waitFor();
    if (await error.isVisible()) {
      throw new Error(`Query failed: ${await error.innerText()}`);
    }
    return await extractTable(results.locator('.ct-tr-group'), '.ct-td');
  }

  async cancelQuery(query: string): Promise<number> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);

    const queryRequest = this.page.waitForRequest(
      request => request.url().includes('druid/v2') && request.method() === 'POST',
    );
    await clickButton(this.page, 'Run');
    await queryRequest;

    const cancelResponse = this.page.waitForResponse(
      response => response.url().includes('druid/v2') && response.request().method() === 'DELETE',
    );
    await this.page.locator('.cancel-label', { hasText: 'Cancel query' }).click();

    return (await cancelResponse).status();
  }
}
