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

import * as playwright from 'playwright-chromium';

import { clickButton, clickText } from '../../util/playwright';
import { extractTable } from '../../util/table';

/**
 * Represents query overview tab.
 */
export class QueryOverview {
  private readonly page: playwright.Page;
  private readonly baseUrl: string;

  constructor(page: playwright.Page, unifiedConsoleUrl: string) {
    this.page = page;
    this.baseUrl = unifiedConsoleUrl + '#workbench';
  }

  async runQuery(query: string): Promise<string[][]> {
    await this.page.goto(this.baseUrl);
    await this.page.reload({ waitUntil: 'networkidle' });

    const input = await this.page.waitForSelector('div.flexible-query-input textarea');
    await input.fill(query);

    await clickButton(this.page, 'Run');
    await this.page.waitForSelector('div.result-table-pane');

    return await extractTable(this.page, 'div.result-table-pane div.rt-tr-group', 'div.rt-td');
  }

  async cancelQuery(query: string): Promise<number> {
    await this.page.goto(this.baseUrl);
    await this.page.reload({ waitUntil: 'networkidle' });

    const input = await this.page.waitForSelector('div.flexible-query-input textarea');
    await input.fill(query);

    await Promise.all([
      this.page.waitForRequest(
        request => request.url().includes('druid/v2') && request.method() === 'POST',
      ),
      clickButton(this.page, 'Run'),
    ]);

    await this.page.waitForSelector('.cancel-label');

    const [resp] = await Promise.all([
      this.page.waitForResponse(
        response => response.url().includes('druid/v2') && response.request().method() === 'DELETE',
      ),

      clickText(this.page, 'Cancel query'),
      this.page.off(
        'requestfinished',
        request => request.url().includes('druid/v2') && request.method() === 'POST',
      ),
      this.page.off(
        'requestfinished',
        request => request.url().includes('druid/v2') && request.method() === 'DELETE',
      ),
    ]);

    return resp.status();
  }
}
