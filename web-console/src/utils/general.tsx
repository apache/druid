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

import { Button, InputGroup, Intent, HTMLSelect } from '@blueprintjs/core';
import { IconNames } from "@blueprintjs/icons";
import * as numeral from "numeral";
import * as React from 'react';
import { Filter, FilterRender } from 'react-table';


export function addFilter(filters: Filter[], id: string, value: string): Filter[] {
  let currentFilter = filters.find(f => f.id === id);
  if (currentFilter) {
    filters = filters.filter(f => f.id !== id);
    if (currentFilter.value !== value) {
      filters = filters.concat({ id, value });
    }
  } else {
    filters = filters.concat({ id, value });
  }
  return filters;
}

export function makeTextFilter(placeholder: string = ''): FilterRender {
  return ({ filter, onChange, key }) => {
    const filterValue = filter ? filter.value : '';
    return <InputGroup
      key={key}
      onChange={(e: any) => onChange(e.target.value)}
      value={filterValue}
      rightElement={filterValue ? <Button icon={IconNames.CROSS} intent={Intent.NONE} minimal={true} onClick={() => onChange('')} /> : undefined}
      placeholder={placeholder}
    />
  }
}

export function makeBooleanFilter(): FilterRender {
  return ({ filter, onChange, key }) => {
    const filterValue = filter ? filter.value : '';
    return <HTMLSelect
      key={key}
      style={{ width: '100%' }}
      onChange={event => onChange(event.target.value)}
      value={filterValue || "all"}
      fill={true}
    >
      <option value="all">Show all</option>
      <option value="true">true</option>
      <option value="false">false</option>
    </HTMLSelect>;
  }
}

// ----------------------------

export function countBy<T>(array: T[], fn: (x: T, index: number) => string = String): Record<string, number> {
  const counts: Record<string, number> = {};
  for (let i = 0; i < array.length; i++) {
    const key = fn(array[i], i);
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

export function lookupBy<T>(array: T[], fn: (x: T, index: number) => string = String): Record<string, T> {
  const lookup: Record<string, T> = {};
  for (let i = 0; i < array.length; i++) {
    const key = fn(array[i], i);
    lookup[key] = array[i];
  }
  return lookup;
}

export function parseList(list: string): string[] {
  if (!list) return [];
  return list.split(',');
}

// ----------------------------

export function formatNumber(n: number): string {
  return numeral(n).format('0,0');
}

export function formatBytes(n: number): string {
  return numeral(n).format('0.00 b');
}

export function formatBytesCompact(n: number): string {
  return numeral(n).format('0.00b');
}

function pad2(str: string | number): string {
  return ('00' + str).substr(-2);
}

export function formatDuration(ms: number): string {
  const timeInHours = Math.floor(ms / 3600000);
  const timeInMin = Math.floor(ms / 60000) % 60;
  const timeInSec = Math.floor(ms / 1000) % 60;
  return timeInHours + ':' + pad2(timeInMin) + ':' + pad2(timeInSec);
}

export function pluralIfNeeded(n: number, singular: string, plural?: string): string {
  if (!plural) plural = singular + 's';
  return `${formatNumber(n)} ${n === 1 ? singular : plural}`;
}

export function getHeadProp(results: Record<string, any>[], prop: string): any {
  if (!results || !results.length) return null;
  return results[0][prop] || null;
}

// ----------------------------

export function localStorageSet(key: string, value: string): void {
  if (typeof localStorage === 'undefined') return;
  localStorage.setItem(key, value);
}

export function localStorageGet(key: string): string | null {
  if (typeof localStorage === 'undefined') return null;
  return localStorage.getItem(key);
}
