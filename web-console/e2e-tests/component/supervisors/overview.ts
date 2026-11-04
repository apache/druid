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

import { clickButton, clickMenuItem, openView } from '../../util/playwright';
import { showStep } from '../../util/steps';
import { extractTableRecords } from '../../util/table';

/**
 * A row of the Supervisors view.
 */
export interface Supervisor {
  readonly id: string;
  readonly datasource: string;
  readonly status: string;
}

/**
 * An action of a supervisor's menu that asks for confirmation, with the text of its confirm button.
 */
const CONFIRMED_ACTIONS = {
  'Suspend': 'Suspend supervisor',
  'Resume': 'Resume supervisor',
  'Hard reset': 'Hard reset supervisor',
  'Terminate': 'Terminate supervisor',
} as const;

export type SupervisorAction = keyof typeof CONFIRMED_ACTIONS;

/**
 * Represents the Supervisors view.
 */
export class SupervisorsOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  private readonly table = () => this.page.locator('.supervisors-view .console-table');

  /**
   * The supervisor `supervisorId` (none, if it doesn't exist), as the view shows it.
   */
  async getSupervisors(supervisorId: string): Promise<Supervisor[]> {
    await openView(this.page, 'supervisors', { supervisor_id: supervisorId });

    const records = await extractTableRecords(this.table());
    await showStep(this.page, `Supervisors view, filtered on ${supervisorId}`);

    return records.map(record => ({
      id: record['Supervisor ID'],
      datasource: record['Datasource'],
      status: record['Status'],
    }));
  }

  /**
   * Runs an action from the supervisor's menu, and confirms it.
   */
  async runAction(supervisorId: string, action: SupervisorAction): Promise<void> {
    await openView(this.page, 'supervisors', { supervisor_id: supervisorId });
    await this.table().locator('.ct-tbody .action-cell .bp6-icon-more').click();
    await clickMenuItem(this.page, action);

    const confirmation = this.page.locator('.bp6-alert');
    await confirmation.waitFor();
    await showStep(this.page, `Supervisors view: ${action}, to confirm`);
    await clickButton(confirmation, CONFIRMED_ACTIONS[action]);
    await confirmation.waitFor({ state: 'detached' });
  }
}
