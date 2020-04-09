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

import * as playwright from 'playwright-core';

import { extractTable } from '../../util/table';

/**
 * Represents query overview tab.
 */
export class QueryOverview {
  private readonly page: playwright.Page;
  private readonly baseUrl: string;

  constructor(page: playwright.Page, unifiedConsoleUrl: string) {
    this.page = page;
    this.baseUrl = unifiedConsoleUrl + '#query';
  }

  async runQuery(query: string): Promise<string[][]> {
    await this.page.goto(this.baseUrl);
    await this.page.reload({ waitUntil: 'networkidle0' });

    const input = await this.page.$('div.query-input textarea');
    await this.setInput(input!, query);
    await this.clickButton('Run');
    await this.page.waitFor('div.query-info');

    return await extractTable(this.page, 'div.query-output div.rt-tr-group', 'div.rt-td');
  }

  private async setInput(input: playwright.ElementHandle<Element>, value: string) {
    await input.fill('');
    await input.type(value);
  }

  private async clickButton(text: string) {
    await this.page.click(`//button/*[contains(text(),"${text}")]`, { waitUntil: 'load' } as any);
  }
}
