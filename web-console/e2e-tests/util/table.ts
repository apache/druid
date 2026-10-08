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

import type { Locator } from '@playwright/test';

/**
 * Reads the data rows (not the rows that pad the table) of a ConsoleTable as text, one array of cells per row.
 * @param table locator of the table (its `.console-table` element, or one containing it)
 */
export async function extractTable(table: Locator): Promise<string[][]> {
  return (await readTable(table)).rows;
}

/**
 * Reads the data rows of a ConsoleTable as text, each row keyed by its column headers. Headers on two lines are read
 * with a space for the line break (like "Datasource name").
 * @param table locator of the table (its `.console-table` element, or one containing it)
 */
export async function extractTableRecords(table: Locator): Promise<Record<string, string>[]> {
  const { headers, rows } = await readTable(table);
  return rows.map(row => Object.fromEntries(headers.map((header, i) => [header, row[i]])));
}

async function readTable(table: Locator): Promise<{ headers: string[]; rows: string[][] }> {
  return await table.evaluate(tableElement => {
    const cellText = (cell: Element) =>
      ((cell.querySelector('.real-text') ?? cell) as HTMLElement).innerText;

    return {
      headers: Array.from(tableElement.querySelectorAll('.ct-thead.-header .ct-th'), header =>
        (header as HTMLElement).innerText.replace(/\s+/g, ' ').trim(),
      ),
      rows: Array.from(tableElement.querySelectorAll('.ct-tbody .ct-tr:not(.-padRow)'), row =>
        Array.from(row.querySelectorAll('.ct-td'), cellText),
      ),
    };
  });
}
