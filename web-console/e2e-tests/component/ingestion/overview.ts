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

import { filterableText, openView } from '../../util/playwright';
import { extractTable } from '../../util/table';

import { IngestionTask } from './task';

/**
 * Ingestion overview task table column identifiers.
 */
enum TaskColumn {
  TASK_ID = 0,
  GROUP_ID,
  TYPE,
  DATASOURCE,
  STATUS,
  CREATED_TIME,
  DURATION,
  LOCATION,
}

/**
 * Represents task tab.
 */
export class TasksOverview {
  private readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  /**
   * The tasks of the datasources whose name contains (the filterable end of) `datasourceName`.
   */
  async getTasks(datasourceName: string): Promise<IngestionTask[]> {
    await openView(this.page, 'tasks', { datasource: filterableText(datasourceName) });

    const data = await extractTable(this.page.locator('.tasks-view .ct-tr-group'), '.ct-td');

    return data.map(
      row =>
        new IngestionTask({
          datasource: row[TaskColumn.DATASOURCE],
          status: row[TaskColumn.STATUS],
        }),
    );
  }
}
