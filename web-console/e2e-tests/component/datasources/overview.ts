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

import {
  clickButton,
  clickMenuItem,
  filterableText,
  getLabeledInput,
  openView,
  setLabeledInput,
} from '../../util/playwright';
import { extractTableRecords } from '../../util/table';
import { readPartitionSpec } from '../load-data/config/partition';

import { CompactionConfig } from './compaction';
import { Datasource } from './datasource';

const SKIP_OFFSET_FROM_LATEST = 'Skip offset from latest';

/**
 * Represents datasource overview tab.
 */
export class DatasourcesOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  private readonly table = () => this.page.locator('.datasources-view .console-table');

  /**
   * The datasources whose name contains (the filterable end of) `datasourceName`.
   */
  async getDatasources(datasourceName: string): Promise<Datasource[]> {
    await openView(this.page, 'datasources', { datasource: filterableText(datasourceName) });

    const records = await extractTableRecords(this.table());

    return records.map(
      record =>
        new Datasource({
          name: record['Datasource name'],
          availability: record['Availability'],
          totalRows: DatasourcesOverview.parseNumber(record['Total rows']),
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
    const dialog = await this.openCompactionConfigurationDialog(datasourceName);

    await setLabeledInput(dialog, SKIP_OFFSET_FROM_LATEST, compactionConfig.skipOffsetFromLatest);
    await compactionConfig.partitionsSpec.apply(this.page);

    await clickButton(dialog, 'Submit');
  }

  async getCompactionConfiguration(datasourceName: string): Promise<CompactionConfig> {
    const dialog = await this.openCompactionConfigurationDialog(datasourceName);

    const skipOffsetFromLatest = await getLabeledInput(dialog, SKIP_OFFSET_FROM_LATEST);
    const partitionsSpec = await readPartitionSpec(this.page);

    await clickButton(dialog.locator('.bp6-dialog-footer'), 'Close');
    return new CompactionConfig({ skipOffsetFromLatest, partitionsSpec: partitionsSpec! });
  }

  private async openCompactionConfigurationDialog(datasourceName: string) {
    await openView(this.page, 'datasources', { datasource: filterableText(datasourceName) });
    await this.table().locator('.ct-tbody .action-cell .bp6-icon-more').click();
    await clickMenuItem(this.page, 'Edit compaction configuration');

    const dialog = this.page.locator('.compaction-config-dialog');
    await dialog.waitFor();
    return dialog;
  }

  async triggerCompaction(): Promise<void> {
    await openView(this.page, 'datasources');
    await this.page.locator('.more-button button').click({ modifiers: ['Alt'] });
    await clickMenuItem(this.page, 'Force compaction run');
    await clickButton(this.page.locator('.bp6-alert'), 'Force compaction run');
  }
}
