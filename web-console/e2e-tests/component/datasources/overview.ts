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
  getLabeledInput,
  openView,
  setLabeledInput,
} from '../../util/playwright';
import { showStep } from '../../util/steps';
import { extractTableRecords } from '../../util/table';
import type { PartitionsSpec } from '../load-data/partitions-spec';
import { applyPartitionsSpec, readPartitionsSpec } from '../load-data/partitions-spec';

/**
 * A row of the Datasources view.
 */
export interface Datasource {
  readonly name: string;
  readonly availability: string;
  readonly totalRows: number;
}

/**
 * What the tests set in the compaction config dialog.
 */
export interface CompactionConfig {
  readonly skipOffsetFromLatest: string;
  readonly partitionsSpec: PartitionsSpec;
}

const SKIP_OFFSET_FROM_LATEST = 'Skip offset from latest';

/**
 * An action of a datasource's menu that asks for confirmation, with the text of its confirm button. The menu of a
 * datasource with used segments has the "unused" actions, the menu of one shown only as unused the others.
 */
const CONFIRMED_ACTIONS = {
  'Mark as unused all segments': 'Mark as unused all segments',
  'Mark as used all segments': 'Mark as used all segments',
  'Delete segments (issue kill task)': 'Permanently delete unused segments',
} as const;

export type DatasourceAction = keyof typeof CONFIRMED_ACTIONS;

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
   * The datasource named `datasourceName` (none, if it doesn't exist yet), as the view shows it.
   * @param showUnused whether to show the datasource also when all of its segments are unused (with "Show unused")
   */
  async getDatasources(
    datasourceName: string,
    { showUnused = false }: { showUnused?: boolean } = {},
  ): Promise<Datasource[]> {
    await this.open(datasourceName, showUnused);

    const records = await extractTableRecords(this.table());
    await showStep(
      this.page,
      `Datasources view, filtered on ${datasourceName}${showUnused ? ', with unused' : ''}`,
    );

    return records.map(record => ({
      name: record['Datasource name'],
      availability: record['Availability'],
      totalRows: Number(record['Total rows'].replace(/,/g, '')),
    }));
  }

  async setCompactionConfiguration(
    datasourceName: string,
    compactionConfig: CompactionConfig,
  ): Promise<void> {
    const dialog = await this.openCompactionConfigurationDialog(datasourceName);

    await setLabeledInput(dialog, SKIP_OFFSET_FROM_LATEST, compactionConfig.skipOffsetFromLatest);
    await applyPartitionsSpec(this.page, compactionConfig.partitionsSpec);
    await showStep(this.page, 'Compaction config dialog, filled in');

    await clickButton(dialog, 'Submit');
  }

  async getCompactionConfiguration(datasourceName: string): Promise<CompactionConfig> {
    const dialog = await this.openCompactionConfigurationDialog(datasourceName);

    const skipOffsetFromLatest = await getLabeledInput(dialog, SKIP_OFFSET_FROM_LATEST);
    const partitionsSpec = await readPartitionsSpec(this.page);
    await showStep(this.page, 'Compaction config dialog, as saved');

    await clickButton(dialog.locator('.bp6-dialog-footer'), 'Close');
    return { skipOffsetFromLatest, partitionsSpec: partitionsSpec! };
  }

  /**
   * Runs an action from the datasource's menu, and confirms it (ticking the checks of what it does, if it asks).
   * @param showUnused whether the datasource is only shown with "Show unused" (all of its segments are unused)
   */
  async runAction(
    datasourceName: string,
    action: DatasourceAction,
    { showUnused = false }: { showUnused?: boolean } = {},
  ): Promise<void> {
    await this.open(datasourceName, showUnused);
    await this.openActionMenu();
    await clickMenuItem(this.page, action);

    const confirmation = this.page.locator('.async-action-dialog');
    await confirmation.waitFor();
    // The checks of what it does (like "I understand that this operation cannot be undone") are switches
    for (const check of await confirmation.locator('.warning-checklist .bp6-switch').all()) {
      await check.click();
    }
    await showStep(this.page, `Datasources view: ${action}, to confirm`);
    await clickButton(confirmation, CONFIRMED_ACTIONS[action]);
    await confirmation.waitFor({ state: 'detached' });
  }

  private async open(datasourceName: string, showUnused: boolean): Promise<void> {
    await openView(this.page, 'datasources', { datasource: datasourceName });
    if (!showUnused) return;

    // Showing the unused datasources fetches them; until then the table shows what it had
    const unusedFetched = this.page.waitForResponse(response =>
      response.url().includes('/druid/coordinator/v1/metadata/datasources?includeUnused'),
    );
    await this.page.locator('.datasources-view').getByText('Show unused', { exact: true }).click();
    await unusedFetched;
    await this.table().locator('.loader').waitFor({ state: 'hidden' });
  }

  private async openActionMenu(): Promise<void> {
    await this.table().locator('.ct-tbody .action-cell .bp6-icon-more').click();
  }

  private async openCompactionConfigurationDialog(datasourceName: string) {
    await this.open(datasourceName, false);
    await this.openActionMenu();
    await clickMenuItem(this.page, 'Edit compaction configuration');

    const dialog = this.page.locator('.compaction-config-dialog');
    await dialog.waitFor();
    return dialog;
  }

  async triggerCompaction(): Promise<void> {
    await openView(this.page, 'datasources');
    await this.page.locator('.more-button button').click({ modifiers: ['Alt'] });
    await clickMenuItem(this.page, 'Force compaction run');
    const confirmation = this.page.locator('.bp6-alert');
    await confirmation.waitFor();
    await showStep(this.page, 'Force compaction run, to confirm');
    await clickButton(confirmation, 'Force compaction run');
  }
}
