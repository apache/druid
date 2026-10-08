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
import { showStep } from '../../util/steps';
import { extractTable } from '../../util/table';

/**
 * Represents the Query view (the workbench).
 */
export class WorkbenchOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  /**
   * Runs a query and returns its results (as text, one array of cells per row). Throws if the query fails.
   */
  async runQuery(query: string): Promise<string[][]> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);
    await clickButton(this.page, 'Run');

    const results = this.page.locator('.result-table-pane');
    const error = this.page.locator('.execution-error-pane');
    // Shown before running a query with the MSQ task engine when the cluster lacks task slots
    const capacityAlert = this.page.locator('.alert-dialog').filter({
      hasText: 'The cluster does not currently have enough available task slots',
    });
    const timeout = 4 * 60 * 1000;
    await results.or(error).or(capacityAlert).waitFor({ timeout });
    if (await capacityAlert.isVisible()) {
      await showStep(this.page, 'Query view: not enough task slots, to confirm');
      await clickButton(capacityAlert, 'Run it anyway');
      await results.or(error).waitFor({ timeout });
    }

    if (await error.isVisible()) {
      await showStep(this.page, 'Query view: the query failed');
      throw new Error(`Query failed: ${await error.innerText()}`);
    }
    await showStep(this.page, 'Query view: the query results');
    return await extractTable(results);
  }

  /**
   * Runs a query, cancels it once it is running and returns the status of the cancel request.
   */
  async cancelQuery(query: string): Promise<number> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);

    // Matched on the query, as the view sends other SQL (like the one of its column tree) as it loads
    const queryRequest = this.page.waitForRequest(
      request =>
        request.url().includes('druid/v2') &&
        request.method() === 'POST' &&
        (request.postData() ?? '').includes(JSON.stringify(query).slice(1, -1)),
    );
    await clickButton(this.page, 'Run');
    await queryRequest;
    await showStep(this.page, 'Query view: the query running');

    const cancelResponse = this.page.waitForResponse(
      response => response.url().includes('druid/v2') && response.request().method() === 'DELETE',
    );
    await this.page.locator('.cancel-label', { hasText: 'Cancel query' }).click();

    const status = (await cancelResponse).status();
    await showStep(this.page, 'Query view: the query canceled');
    return status;
  }
}
