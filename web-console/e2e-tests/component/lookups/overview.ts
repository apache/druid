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

import { button, clickButton, openView, setLabeledInput } from '../../util/playwright';
import { showStep } from '../../util/steps';
import { extractTableRecords } from '../../util/table';

/**
 * A row of the Lookups view.
 */
export interface Lookup {
  readonly name: string;
  readonly tier: string;
  readonly type: string;
}

/**
 * A lookup on a table of a database (a cachedNamespace lookup with a jdbc extraction namespace).
 */
export interface JdbcLookup {
  readonly name: string;
  readonly connectUri: string;
  readonly table: string;
  readonly keyColumn: string;
  readonly valueColumn: string;
  readonly timestampColumn?: string;
}

/**
 * Represents the Lookups view.
 */
export class LookupsOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  private readonly table = () => this.page.locator('.lookups-view .console-table');

  /**
   * Opens the view, initializing the lookups (their config, with the default tier) if the cluster has none yet, as
   * the view asks.
   */
  async open(): Promise<void> {
    await this.load();

    const initialize = button(this.page, 'Initialize lookups');
    if (await initialize.isVisible()) {
      await showStep(this.page, 'Lookups view, not initialized');
      await initialize.click();
      await this.table().waitFor();
    }
  }

  /**
   * Opens the view and waits for it to fetch the lookups, as it shows "No lookups" (and the "Add lookup" button) also
   * while it does. It reloads, to be sure the fetch it waits for is one that starts after this.
   */
  private async load(filter?: Record<string, string>): Promise<void> {
    await openView(this.page, 'lookups', filter);
    const lookupsFetched = this.page.waitForResponse(response =>
      response.url().includes('/druid/coordinator/v1/lookups/config/all'),
    );
    await this.page.reload();
    await lookupsFetched;
    await this.table().or(button(this.page, 'Initialize lookups')).waitFor();
    await this.table().locator('.loader').waitFor({ state: 'hidden' });
  }

  async addJdbcLookup(lookup: JdbcLookup): Promise<void> {
    await this.open();
    await clickButton(this.page, 'Add lookup');

    const dialog = this.page.locator('.lookup-edit-dialog');
    await setLabeledInput(dialog, 'Name', lookup.name);
    await setLabeledInput(dialog, 'Type', 'cachedNamespace');
    await setLabeledInput(dialog, 'Extraction type', 'jdbc');
    await setLabeledInput(dialog, 'Connect URI', lookup.connectUri);
    await setLabeledInput(dialog, 'Table', lookup.table);
    await setLabeledInput(dialog, 'Key column', lookup.keyColumn);
    await setLabeledInput(dialog, 'Value column', lookup.valueColumn);
    if (lookup.timestampColumn) {
      await setLabeledInput(dialog, 'Timestamp column', lookup.timestampColumn);
    }
    await showStep(this.page, 'Lookup dialog, filled in');
    await clickButton(dialog, 'Submit');
    await dialog.waitFor({ state: 'detached' });
  }

  /**
   * The lookup named `lookupName` (none, if it doesn't exist), as the view shows it.
   */
  async getLookups(lookupName: string): Promise<Lookup[]> {
    await this.load({ lookup_name: lookupName });

    const records = await extractTableRecords(this.table());
    await showStep(this.page, `Lookups view, filtered on ${lookupName}`);

    return records.map(record => ({
      name: record['Lookup name'],
      tier: record['Lookup tier'],
      type: record['Type'],
    }));
  }
}
