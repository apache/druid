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

import { Datasource } from './datasource';

/**
 * Datasource overview table column identifiers.
 */
enum DatasourceColumn {
  NAME = 0,
  AVAILABILITY,
  SEGMENT_LOAD_DROP,
  RETENTION,
  REPLICATED_SIZE,
  SIZE,
  COMPACTION,
  AVG_SEGMENT_SIZE,
  NUM_ROWS,
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
          numRows: DatasourcesOverview.parseNumber(row[DatasourceColumn.NUM_ROWS]),
        }),
    );
  }

  private static parseNumber(text: string): number {
    return Number(text.replace(/,/g, ''));
  }
}
