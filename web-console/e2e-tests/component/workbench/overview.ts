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

import type { Locator, Page, Request } from '@playwright/test';

import { expect } from '../../util/fixtures';
import { clickButton, clickMenuItem, openView, setQueryInput } from '../../util/playwright';
import { showStep } from '../../util/steps';
import { extractTable } from '../../util/table';

/**
 * How to run a query.
 */
export interface QueryOptions {
  /** The engine to pick in the view's engine menu, by its name there (like "SQL (Dart)"), rather than "Auto" */
  readonly engine?: string;
}

/**
 * What the view sent to run a query.
 */
export interface SubmittedQuery {
  /** The engine of the query's context, which the view sets for the engine picked (like "msq-dart") */
  readonly engine?: string;
  /** The ID that the view gives the query (SQL queries, natively or with Dart), to cancel it */
  readonly sqlQueryId?: string;
}

/**
 * Represents the Query view (the workbench).
 */
export class WorkbenchOverview {
  private readonly page: Page;

  /** What the view sent to run the last query run (or started) */
  lastSubmittedQuery: SubmittedQuery | undefined;

  constructor(page: Page) {
    this.page = page;
  }

  /**
   * Runs a query and returns its results (as text, one array of cells per row). Throws if the query fails.
   */
  async runQuery(query: string, options: QueryOptions = {}): Promise<string[][]> {
    const results = this.page.locator('.result-table-pane');
    await this.run(query, options, results);
    await showStep(this.page, 'Query view: the query results');
    return await extractTable(results);
  }

  /**
   * Runs an ingestion query (INSERT or REPLACE, run as an MSQ task) and returns what the view says when it is done
   * (like "39,244 rows inserted into ..."). Throws if the query fails.
   */
  async runIngestQuery(query: string, options: QueryOptions = {}): Promise<string> {
    const ingestSuccess = this.page.locator('.ingest-success-pane');
    await this.run(query, options, ingestSuccess);
    await showStep(this.page, 'Query view: the data ingested');
    return await ingestSuccess.innerText();
  }

  /**
   * Starts a query, without waiting for it to finish, and returns what the view sent to run it.
   */
  async startQuery(query: string, options: QueryOptions = {}): Promise<SubmittedQuery> {
    await this.submit(query, options);
    await showStep(this.page, 'Query view: the query running');
    return this.lastSubmittedQuery!;
  }

  private async run(query: string, options: QueryOptions, done: Locator): Promise<void> {
    await this.submit(query, options);

    const error = this.page.locator('.execution-error-pane');
    await done.or(error).waitFor({ timeout: QUERY_TIMEOUT });
    if (await error.isVisible()) {
      await showStep(this.page, 'Query view: the query failed');
      throw new Error(`Query failed: ${await error.innerText()}`);
    }
  }

  private async submit(query: string, options: QueryOptions): Promise<void> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);
    if (options.engine) await this.pickEngine(options.engine);
    // The view shows the result of the last query until it sends the new one (even after a reload)
    const queryRequest = this.page.waitForRequest(request => isQueryRequest(request, query), {
      timeout: QUERY_TIMEOUT,
    });
    await clickButton(this.page, 'Run');

    const capacityAlert = getCapacityAlert(this.page);
    await Promise.race([
      queryRequest,
      capacityAlert.waitFor({ timeout: QUERY_TIMEOUT }).catch(() => {}),
    ]);
    if (await capacityAlert.isVisible()) {
      await runAnyway(this.page, capacityAlert);
    }

    const context = (await queryRequest).postDataJSON()?.context ?? {};
    this.lastSubmittedQuery = { engine: context.engine, sqlQueryId: context.sqlQueryId };
  }

  private async pickEngine(engine: string): Promise<void> {
    const engineButton = this.page.getByRole('button', { name: /^Engine: / });
    await engineButton.click();
    await clickMenuItem(this.page, engine);
    // The menu stays open (to pick other settings in it)
    await this.page.keyboard.press('Escape');
    await expect(engineButton).toHaveText(`Engine: ${engine}`);
    await showStep(this.page, `Query view: engine ${engine} picked`);
  }

  /**
   * Runs a query, cancels it once it is running and returns the status of the cancel request.
   */
  async cancelQuery(query: string): Promise<number> {
    await openView(this.page, 'workbench');

    await setQueryInput(this.page, query);

    const queryRequest = this.page.waitForRequest(request => isQueryRequest(request, query));
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

const QUERY_TIMEOUT = 4 * 60 * 1000;

/**
 * Whether the request runs the query. Matched on the query (its first line), as the view sends other SQL (like the one
 * of its column tree) as it loads.
 */
function isQueryRequest(request: Request, query: string): boolean {
  return (
    request.url().includes('druid/v2') &&
    request.method() === 'POST' &&
    (request.postData() ?? '').includes(JSON.stringify(query.split('\n')[0]).slice(1, -1))
  );
}

/**
 * The alert that the console shows before running a query with the MSQ task engine when the cluster lacks the task
 * slots for it.
 */
function getCapacityAlert(page: Page): Locator {
  return page.locator('.alert-dialog').filter({
    hasText: 'The cluster does not currently have enough available task slots',
  });
}

async function runAnyway(page: Page, capacityAlert: Locator): Promise<void> {
  await showStep(page, 'Not enough task slots, to confirm');
  await clickButton(capacityAlert, 'Run it anyway');
}

/**
 * Waits for `done`, after starting an MSQ task (like the SQL data loader does), running it anyway if the console
 * asks because the cluster lacks the task slots for it.
 */
export async function waitForTaskSlots(page: Page, done: Locator): Promise<void> {
  const capacityAlert = getCapacityAlert(page);
  await done.or(capacityAlert).waitFor({ timeout: QUERY_TIMEOUT });
  if (await capacityAlert.isVisible()) {
    await runAnyway(page, capacityAlert);
    await done.waitFor({ timeout: QUERY_TIMEOUT });
  }
}
