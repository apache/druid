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

import type { Locator, Page } from '@playwright/test';

import { clickButton, clickMenuItem } from '../../util/playwright';
import { showStep } from '../../util/steps';

const STATES = ['ACCEPTED', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELED'];

/**
 * Represents the "Current Dart queries" panel of the Query view, which lists the Dart queries (running and recent).
 */
export class CurrentDartPanel {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  private readonly panel = () => this.page.locator('.current-dart-panel');

  /**
   * Shows the panel (in the Query view, which must be open), if it isn't shown. The view remembers it.
   */
  async show(): Promise<void> {
    if (await this.panel().isVisible()) return;
    await this.page.locator('[data-tooltip="Open helper panels"]').click();
    await this.page.getByText('Current Dart query panel', { exact: true }).click();
    await this.panel().waitFor();
  }

  /**
   * The state of the query (like RUNNING or SUCCESS) as the panel shows it, or null when it doesn't list it (yet).
   */
  async getQueryState(sqlQueryId: string): Promise<string | null> {
    const entry = this.entry(sqlQueryId);
    if (!(await entry.count())) return null;

    const iconClasses = (await entry.locator('.status-icon').getAttribute('class')) ?? '';
    return STATES.find(state => iconClasses.split(' ').includes(state.toLowerCase())) ?? null;
  }

  /**
   * Opens the execution details of the query (its report) and returns the dialog.
   */
  async showDetails(sqlQueryId: string): Promise<Locator> {
    await this.entry(sqlQueryId).click();
    await clickMenuItem(this.page, 'Show details');

    const dialog = this.page.locator('.execution-details-dialog');
    await dialog.waitFor();
    return dialog;
  }

  /**
   * Cancels the query, confirming it.
   */
  async cancelQuery(sqlQueryId: string): Promise<void> {
    await this.entry(sqlQueryId).click();
    await clickMenuItem(this.page, 'Cancel query');

    const confirmation = this.page.locator('.bp6-alert');
    await confirmation.waitFor();
    await showStep(this.page, 'Current Dart queries panel: cancel the query, to confirm');
    await clickButton(confirmation, 'Cancel query');
    await confirmation.waitFor({ state: 'detached' });
  }

  private entry(sqlQueryId: string): Locator {
    return this.panel()
      .locator('.work-entry')
      .filter({ has: this.page.locator(`[data-tooltip$="SQL ID: ${sqlQueryId}"]`) });
  }
}
