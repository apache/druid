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

export class LocalStorageBackedArray<T> {
  key: LocalStorageKeys;
  storedArray: T[];

  constructor(key: LocalStorageKeys, array?: T[]) {
    this.key = key;
    if (!Array.isArray(array)) {
      this.storedArray = this.getDataFromStorage();
    } else {
      this.storedArray = array;
      this.setDataInStorage();
    }
  }

  private getDataFromStorage(): T[] {
    try {
      const possibleArray: any = JSON.parse(String(localStorageGet(this.key)));
      if (!Array.isArray(possibleArray)) return [];
      return possibleArray;
    } catch {
      return [];
    }
  }

  private setDataInStorage(): void {
    localStorageSet(this.key, JSON.stringify(this.storedArray));
  }

  toggle(value: T): LocalStorageBackedArray<T> {
    let toggledArray;
    if (this.storedArray.includes(value)) {
      toggledArray = this.storedArray.filter(c => c !== value);
    } else {
      toggledArray = this.storedArray.concat(value);
    }
    return new LocalStorageBackedArray<T>(this.key, toggledArray);
  }

  exists(value: T): boolean {
    return !this.storedArray.includes(value);
  }
}
