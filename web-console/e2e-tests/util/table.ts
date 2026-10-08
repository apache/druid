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
 * Reads the rows of a table as text, skipping the blank rows that pad it.
 * @param rows locator matching the table's rows
 * @param cellSelector CSS selector for a cell within a row
 */
export async function extractTable(rows: Locator, cellSelector: string): Promise<string[][]> {
  return await rows.evaluateAll((rowElements, cellSelector) => {
    const BLANK_VALUE = '\xa0';
    const data: string[][] = [];
    for (const row of rowElements) {
      const values = Array.from(row.querySelectorAll(cellSelector)).map(c => {
        const realText = c.querySelector('.real-text');
        return ((realText ?? c) as HTMLElement).innerText;
      });
      if (!values.every(value => value === BLANK_VALUE)) {
        data.push(values);
      }
    }
    return data;
  }, cellSelector);
}
