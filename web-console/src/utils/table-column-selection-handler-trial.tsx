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

import { LocalStorageBackedArray } from './localStorageBackedArray';

export class TableColumnSelectionHandlerTrial {
  hiddenColumns: string[];
  updateComponent: (newArray: string[]) => void;

  constructor(storedArray: LocalStorageBackedArray, updateComponent: (newArray: string[]) => void) {
    this.updateComponent = updateComponent;
    storedArray.getDataFromStorage();
    this.hiddenColumns = storedArray.copy();
  }

  changeTableColumnSelector(column: string): void {
    let newSelections: string[];
    if (this.hiddenColumns.includes(column)) {
      newSelections = this.hiddenColumns.filter(c => c !== column);
    } else {
      newSelections = this.hiddenColumns.concat(column);
    }
    this.hiddenColumns = newSelections;
    this.updateComponent(this.hiddenColumns);
  }

  showColumn(column: string): boolean {
    return !this.hiddenColumns.includes(column);
  }
}
