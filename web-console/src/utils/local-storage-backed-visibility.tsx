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

import type { LocalStorageKeys } from './local-storage-keys';
import { localStorageGetJson, localStorageSetJson } from './local-storage-keys';

type Visibility = Record<string, boolean>;

export class LocalStorageBackedVisibility {
  private readonly key: LocalStorageKeys;
  private readonly visibility: Visibility;
  private readonly defaultHidden: string[];

  constructor(key: LocalStorageKeys, defaultHidden: string[] = [], visibility?: Visibility) {
    this.key = key;
    if (visibility) {
      this.visibility = visibility;
    } else {
      const visibilityFromStorage = localStorageGetJson(this.key);
      if (visibilityFromStorage && typeof visibilityFromStorage === 'object') {
        if (Array.isArray(visibilityFromStorage)) {
          // Backwards compatability from when we used to store a hidden columns array.
          this.visibility = {};
          for (const k of visibilityFromStorage) {
            this.visibility[k] = false;
          }
        } else {
          this.visibility = visibilityFromStorage;
        }
      } else {
        this.visibility = {};
      }
    }

    this.defaultHidden = defaultHidden;
  }

  public getHiddenColumns(): string[] {
    const { visibility, defaultHidden } = this;
    const visibilityKeys = Object.keys(visibility);
    return visibilityKeys
      .filter(k => !visibility[k])
      .concat(defaultHidden.filter(k => !Object.hasOwn(visibility, k)));
  }

  public toggle(value: string): LocalStorageBackedVisibility {
    const { key, visibility, defaultHidden } = this;
    const newVisibility: Visibility = { ...visibility, [value]: !this.shown(value) };
    localStorageSetJson(key, newVisibility);
    return new LocalStorageBackedVisibility(this.key, defaultHidden, newVisibility);
  }

  public shown(value: string): boolean {
    return this.visibility[value] ?? !this.defaultHidden.includes(value);
  }
}
