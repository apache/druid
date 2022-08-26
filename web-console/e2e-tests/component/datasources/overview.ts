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

import { clickButton, getLabeledInput, setLabeledInput } from '../../util/playwright';
import { extractTable } from '../../util/table';
import { readPartitionSpec } from '../load-data/config/partition';

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
  SEGMENT_ROWS,
  // SEGMENT_SIZE, (Hidden by default)
  // SEGMENT_GRANULARITY, (Hidden by default)
  TOTAL_ROWS,
  AVG_ROW_SIZE,
  REPLICATED_SIZE,
  COMPACTION,
  PERCENT_COMPACTED,
  LEFT_TO_BE_COMPACTED,
  RETENTION,
  ACTIONS,
}

const SKIP_OFFSET_FROM_LATEST = 'Skip offset from latest';

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
    await this.page.reload({ waitUntil: 'networkidle' });

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
    await this.openCompactionConfigurationDialog(datasourceName);

    await setLabeledInput(
      this.page,
      SKIP_OFFSET_FROM_LATEST,
      compactionConfig.skipOffsetFromLatest,
    );
    await compactionConfig.partitionsSpec.apply(this.page);

    await clickButton(this.page, 'Submit');
  }

  private async openCompactionConfigurationDialog(datasourceName: string): Promise<void> {
    await this.openEditActions(datasourceName);
    await this.clickMenuItem('Edit compaction configuration');
    await this.page.waitForSelector('div.compaction-dialog');
  }

  private async clickMenuItem(text: string): Promise<void> {
    const menuItemSelector = `//a[*[contains(text(),"${text}")]]`;
    await this.page.click(menuItemSelector);
  }

  async getCompactionConfiguration(datasourceName: string): Promise<CompactionConfig> {
    await this.openCompactionConfigurationDialog(datasourceName);

    const skipOffsetFromLatest = await getLabeledInput(this.page, SKIP_OFFSET_FROM_LATEST);
    const partitionsSpec = await readPartitionSpec(this.page);

    await clickButton(this.page, 'Close');
    return new CompactionConfig({ skipOffsetFromLatest, partitionsSpec: partitionsSpec! });
  }

  private async openEditActions(datasourceName: string): Promise<void> {
    const datasources = await this.getDatasources();
    const index = datasources.findIndex(t => t.name === datasourceName);
    if (index < 0) {
      throw new Error(`Could not find datasource: ${datasourceName}`);
    }

    const editActions = await this.page.$$('.action-cell span[icon=more]');
    await editActions[index].click();
    await this.waitForPopupMenu();
  }

  private async waitForPopupMenu(): Promise<void> {
    await this.page.waitForSelector('ul.bp4-menu');
  }

  async triggerCompaction(): Promise<void> {
    await this.page.goto(this.baseUrl);
    await this.clickMoreButton({ modifiers: ['Alt'] });
    await this.clickMenuItem('Force compaction run');
    await clickButton(this.page, 'Force compaction run');
  }

  private async clickMoreButton(options: any): Promise<void> {
    await this.page.click('.more-button button', options);
    await this.waitForPopupMenu();
  }
}
