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

import { CompactionConfig } from './compaction';
import { Datasource } from './datasource';

/**
 * Datasource overview table column identifiers.
 */
enum DatasourceColumn {
  NAME = 0,
  AVAILABILITY,
  SEGMENT_LOAD_DROP,
  TOTAL_DATA_SIZE,
  SEGMENT_SIZE,
  TOTAL_ROWS,
  AVG_ROW_SIZE,
  REPLICATED_SIZE,
  COMPACTION,
  RETENTION,
  ACTIONS,
}

/**
 * Represents datasource overview tab.
 */
export class DatasourcesOverview {
  private readonly page: playwright.Page;
  private readonly baseUrl: string;

  constructor(page: playwright.Page, unifiedConsoleUrl: string) {
    this.page = page;
    this.baseUrl = unifiedConsoleUrl + '#datasources';
  }

  async getDatasources(): Promise<Datasource[]> {
    await this.page.goto(this.baseUrl);
    await this.page.reload({ waitUntil: 'networkidle0' });

    const data = await extractTable(this.page, 'div div.rt-tr-group', 'div.rt-td');

    return data.map(
      row =>
        new Datasource({
          name: row[DatasourceColumn.NAME],
          availability: row[DatasourceColumn.AVAILABILITY],
          totalRows: DatasourcesOverview.parseNumber(row[DatasourceColumn.TOTAL_ROWS]),
        }),
    );
  }

  private static parseNumber(text: string): number {
    return Number(text.replace(/,/g, ''));
  }

  async setCompactionConfiguration(
    datasourceName: string,
    compactionConfig: CompactionConfig,
  ): Promise<void> {
    await this.openEditActions(datasourceName);

    await this.page.click('"Edit compaction configuration"');
    await this.setInput('Skip offset from latest', compactionConfig.skipOffsetFromLatest);
    await compactionConfig.partitionsSpec.apply(this.page);

    await this.clickButton('Submit');
  }

  private async openEditActions(datasourceName: string): Promise<void> {
    const datasources = await this.getDatasources();
    const index = datasources.findIndex(t => t.name === datasourceName);
    if (index < 0) {
      throw new Error(`Could not find datasource: ${datasourceName}`);
    }

    const editActions = await this.page.$$('span[icon=wrench]');
    editActions[index].click();
    await this.page.waitFor(5000);
  }

  private async setInput(label: string, value: string) {
    const input = await this.page.$(`//*[text()="${label}"]/following-sibling::div//input`);
    await input!.fill('');
    await input!.type(value);
  }

  private async clickButton(text: string) {
    await this.page.click(`//button/*[contains(text(),"${text}")]`, { waitUntil: 'load' } as any);
  }
}
