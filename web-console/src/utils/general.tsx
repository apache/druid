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

import { Button, HTMLSelect, InputGroup, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import copy from 'copy-to-clipboard';
import FileSaver from 'file-saver';
import hasOwnProp from 'has-own-prop';
import numeral from 'numeral';
import React from 'react';
import { Filter, FilterRender } from 'react-table';

import { AppToaster } from '../singletons/toaster';
export function wait(ms: number): Promise<void> {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}

export function addFilter(filters: Filter[], id: string, value: string): Filter[] {
  return addFilterRaw(filters, id, `"${value}"`);
}

export function addFilterRaw(filters: Filter[], id: string, value: string): Filter[] {
  const currentFilter = filters.find(f => f.id === id);
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

export function makeTextFilter(placeholder = ''): FilterRender {
  return ({ filter, onChange, key }) => {
    const filterValue = filter ? filter.value : '';
    return (
      <InputGroup
        key={key}
        onChange={(e: any) => onChange(e.target.value)}
        value={filterValue}
        rightElement={
          filterValue && <Button icon={IconNames.CROSS} minimal onClick={() => onChange('')} />
        }
        placeholder={placeholder}
      />
    );
  };
}

export function makeBooleanFilter(): FilterRender {
  return ({ filter, onChange, key }) => {
    const filterValue = filter ? filter.value : '';
    return (
      <HTMLSelect
        key={key}
        style={{ width: '100%' }}
        onChange={(event: any) => onChange(event.target.value)}
        value={filterValue || 'all'}
        fill
      >
        <option value="all">Show all</option>
        <option value="true">true</option>
        <option value="false">false</option>
      </HTMLSelect>
    );
  };
}

// ----------------------------

interface NeedleAndMode {
  needle: string;
  mode: 'exact' | 'prefix';
}

function getNeedleAndMode(input: string): NeedleAndMode {
  if (input.startsWith(`"`) && input.endsWith(`"`)) {
    return {
      needle: input.slice(1, -1),
      mode: 'exact',
    };
  }
  return {
    needle: input.startsWith(`"`) ? input.substring(1) : input,
    mode: 'prefix',
  };
}

export function booleanCustomTableFilter(filter: Filter, value: any): boolean {
  if (value == null) return false;
  const haystack = String(value).toLowerCase();
  const needleAndMode: NeedleAndMode = getNeedleAndMode(filter.value.toLowerCase());
  const needle = needleAndMode.needle;
  if (needleAndMode.mode === 'exact') {
    return needle === haystack;
  }
  return haystack.startsWith(needle);
}

export function sqlQueryCustomTableFilter(filter: Filter): string {
  const columnName = JSON.stringify(filter.id);
  const needleAndMode: NeedleAndMode = getNeedleAndMode(filter.value);
  const needle = needleAndMode.needle;
  if (needleAndMode.mode === 'exact') {
    return `${columnName} = '${needle}'`;
  } else {
    return `LOWER(${columnName}) LIKE LOWER('${needle}%')`;
  }
}

// ----------------------------

export function caseInsensitiveContains(testString: string, searchString: string): boolean {
  if (!searchString) return true;
  return testString.toLowerCase().includes(searchString.toLowerCase());
}

// ----------------------------

export function countBy<T>(
  array: T[],
  fn: (x: T, index: number) => string = String,
): Record<string, number> {
  const counts: Record<string, number> = {};
  for (let i = 0; i < array.length; i++) {
    const key = fn(array[i], i);
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function identity(x: any): any {
  return x;
}

export function lookupBy<T, Q>(
  array: T[],
  keyFn: (x: T, index: number) => string = String,
  valueFn: (x: T, index: number) => Q = identity,
): Record<string, Q> {
  const lookup: Record<string, Q> = {};
  const n = array.length;
  for (let i = 0; i < n; i++) {
    const a = array[i];
    lookup[keyFn(a, i)] = valueFn(a, i);
  }
  return lookup;
}

export function mapRecord<T, Q>(
  record: Record<string, T>,
  fn: (value: T, key: string) => Q,
): Record<string, Q> {
  const newRecord: Record<string, Q> = {};
  const keys = Object.keys(record);
  for (const key of keys) {
    newRecord[key] = fn(record[key], key);
  }
  return newRecord;
}

export function groupBy<T, Q>(
  array: readonly T[],
  keyFn: (x: T, index: number) => string,
  aggregateFn: (xs: readonly T[], key: string) => Q,
): Q[] {
  const buckets: Record<string, T[]> = {};
  const n = array.length;
  for (let i = 0; i < n; i++) {
    const value = array[i];
    const key = keyFn(value, i);
    buckets[key] = buckets[key] || [];
    buckets[key].push(value);
  }
  return Object.keys(buckets).map(key => aggregateFn(buckets[key], key));
}

export function uniq(array: readonly string[]): string[] {
  const seen: Record<string, boolean> = {};
  return array.filter(s => {
    if (hasOwnProp(seen, s)) {
      return false;
    } else {
      seen[s] = true;
      return true;
    }
  });
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

// ----------------------------

export function parseJson(json: string): any {
  try {
    return JSON.parse(json);
  } catch (e) {
    return undefined;
  }
}

export function validJson(json: string): boolean {
  try {
    JSON.parse(json);
    return true;
  } catch (e) {
    return false;
  }
}

// stringify JSON to string; if JSON is null, parse empty string ""
export function stringifyJson(item: any): string {
  if (item != null) {
    return JSON.stringify(item, null, 2);
  } else {
    return '';
  }
}

// parse string to JSON object; if string is empty, return null
export function parseStringToJson(s: string): JSON | null {
  if (s === '') {
    return null;
  } else {
    return JSON.parse(s);
  }
}

export function filterMap<T, Q>(xs: T[], f: (x: T, i: number) => Q | undefined): Q[] {
  return xs.map(f).filter((x: Q | undefined) => typeof x !== 'undefined') as Q[];
}

export function compact<T>(xs: (T | undefined | false | null | '')[]): T[] {
  return xs.filter(Boolean) as T[];
}

export function assemble<T>(...xs: (T | undefined | false | null | '')[]): T[] {
  return xs.filter(Boolean) as T[];
}

export function alphanumericCompare(a: string, b: string): number {
  return String(a).localeCompare(b, undefined, { numeric: true });
}

export function sortWithPrefixSuffix(
  things: string[],
  prefix: string[],
  suffix: string[],
  cmp: null | ((a: string, b: string) => number),
): string[] {
  const pre = uniq(prefix.filter(x => things.includes(x)));
  const mid = things.filter(x => !prefix.includes(x) && !suffix.includes(x));
  const post = uniq(suffix.filter(x => things.includes(x)));
  return pre.concat(cmp ? mid.sort(cmp) : mid, post);
}

// ----------------------------

export function downloadFile(text: string, type: string, filename: string): void {
  let blobType: string = '';
  switch (type) {
    case 'json':
      blobType = 'application/json';
      break;
    case 'tsv':
      blobType = 'text/tab-separated-values';
      break;
    default:
      // csv
      blobType = `text/${type}`;
  }
  const blob = new Blob([text], {
    type: blobType,
  });
  FileSaver.saveAs(blob, filename);
}

export function escapeSqlIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

export function copyAndAlert(copyString: string, alertMessage: string): void {
  copy(copyString, { format: 'text/plain' });
  AppToaster.show({
    message: alertMessage,
    intent: Intent.SUCCESS,
  });
}

export function delay(ms: number) {
  return new Promise(resolve => {
    setTimeout(resolve, ms);
  });
}
