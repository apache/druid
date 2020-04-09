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

/**
 * Extracts an HTML table into a text representation.
 * @param page Playwright page from which to extract HTML table
 * @param tableSelector Playwright selector for table
 * @param rowSelector Playwright selector for table row
 */
export async function extractTable(
  page: playwright.Page,
  tableSelector: string,
  rowSelector: string,
): Promise<string[][]> {
  await page.waitFor(tableSelector);

  return page.evaluate(
    (tableSelector, rowSelector) => {
      const BLANK_VALUE = '\xa0';
      const data = [];
      const rows = document.querySelectorAll(tableSelector);
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const columns = row.querySelectorAll(rowSelector);
        const values = Array.from(columns).map(c => (c as HTMLElement).innerText);
        if (!values.every(value => value === BLANK_VALUE)) {
          data.push(values);
        }
      }
      return data;
    },
    tableSelector,
    rowSelector,
  );
}
