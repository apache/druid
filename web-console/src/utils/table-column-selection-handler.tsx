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

import { localStorageGet, LocalStorageKeys, localStorageSet } from '../utils';

export class TableColumnSelectionHandler {
  tableName: LocalStorageKeys;
  hiddenColumns: string[];
  updateComponent: () => void;

  constructor(tableName: LocalStorageKeys, updateComponent: () => void) {
    this.tableName = tableName;
    this.updateComponent = updateComponent;
    this.getHiddenTableColumns();
  }

  getHiddenTableColumns(): void {
    const stringValue: string | null = localStorageGet(this.tableName);
    try {
      const selections = JSON.parse(String(stringValue));
      if (!Array.isArray(selections)) {
        this.hiddenColumns = [];
      } else {
        this.hiddenColumns = selections;
      }
    } catch (e) {
      this.hiddenColumns = [];
    }
  }

  changeTableColumnSelector(column: string): void {
    let newSelections: string[];
    if (this.hiddenColumns.includes(column)) {
      newSelections = this.hiddenColumns.filter(c => c !== column);
    } else {
      newSelections = this.hiddenColumns.concat(column);
    }
    this.hiddenColumns = newSelections;
    this.updateComponent();
    localStorageSet(this.tableName, JSON.stringify(newSelections));
  }

  showColumn(column: string): boolean {
    return !this.hiddenColumns.includes(column);
  }
}
