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
import { SqlExpression, SqlFunction, SqlLiteral, SqlRef } from 'druid-query-toolkit';
import FileSaver from 'file-saver';
import hasOwnProp from 'has-own-prop';
import * as JSONBig from 'json-bigint-native';
import numeral from 'numeral';
import React from 'react';
import { Filter, FilterRender } from 'react-table';

import { AppToaster } from '../singletons';

export const STANDARD_TABLE_PAGE_SIZE = 50;
export const STANDARD_TABLE_PAGE_SIZE_OPTIONS = [50, 100, 200];

export const SMALL_TABLE_PAGE_SIZE = 25;
export const SMALL_TABLE_PAGE_SIZE_OPTIONS = [25, 50, 100];

// These constants are used to make sure that they are not constantly recreated thrashing the pure components
export const EMPTY_OBJECT: any = {};
export const EMPTY_ARRAY: any[] = [];

export type NumberLike = number | BigInt;

export function isNumberLikeNaN(x: NumberLike): boolean {
  return isNaN(Number(x));
}

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
  return function TextFilter({ filter, onChange, key }) {
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
  return function BooleanFilter({ filter, onChange, key }) {
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
  mode: 'exact' | 'includes';
}

export function getNeedleAndMode(filter: Filter): NeedleAndMode {
  const input = filter.value;
  if (input.startsWith(`"`) && input.endsWith(`"`)) {
    return {
      needle: input.slice(1, -1),
      mode: 'exact',
    };
  } else {
    return {
      needle: input.replace(/^"/, '').toLowerCase(),
      mode: 'includes',
    };
  }
}

export function booleanCustomTableFilter(filter: Filter, value: any): boolean {
  if (value == null) return false;
  const needleAndMode: NeedleAndMode = getNeedleAndMode(filter);
  const needle = needleAndMode.needle;
  if (needleAndMode.mode === 'exact') {
    return needle === String(value);
  } else {
    return String(value).toLowerCase().includes(needle);
  }
}

export function sqlQueryCustomTableFilter(filter: Filter): SqlExpression {
  const needleAndMode: NeedleAndMode = getNeedleAndMode(filter);
  const needle = needleAndMode.needle;
  if (needleAndMode.mode === 'exact') {
    return SqlRef.columnWithQuotes(filter.id).equal(SqlLiteral.create(needle));
  } else {
    return SqlFunction.simple('LOWER', [SqlRef.columnWithQuotes(filter.id)]).like(
      SqlLiteral.create(`%${needle}%`),
    );
  }
}

// ----------------------------

export function caseInsensitiveContains(testString: string, searchString: string): boolean {
  if (!searchString) return true;
  return testString.toLowerCase().includes(searchString.toLowerCase());
}

export function oneOf<T>(thing: T, ...options: T[]): boolean {
  return options.includes(thing);
}

export function typeIs<T extends { type?: S }, S = string>(...options: S[]): (x: T) => boolean {
  return x => {
    if (x.type == null) return false;
    return options.includes(x.type);
  };
}

export function without<T>(xs: readonly T[], x: T | undefined): T[] {
  return xs.filter(i => i !== x);
}

export function change<T>(xs: readonly T[], from: T, to: T): T[] {
  return xs.map(x => (x === from ? to : x));
}

// ----------------------------

export function countBy<T>(
  array: readonly T[],
  fn: (x: T, index: number) => string = String,
): Record<string, number> {
  const counts: Record<string, number> = {};
  for (let i = 0; i < array.length; i++) {
    const key = fn(array[i], i);
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

function identity<T>(x: T): T {
  return x;
}

export function lookupBy<T, Q = T>(
  array: readonly T[],
  keyFn: (x: T, index: number) => string = String,
  valueFn?: (x: T, index: number) => Q,
): Record<string, Q> {
  if (!valueFn) valueFn = identity as any;
  const lookup: Record<string, Q> = {};
  const n = array.length;
  for (let i = 0; i < n; i++) {
    const a = array[i];
    lookup[keyFn(a, i)] = valueFn!(a, i);
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

// ----------------------------

export function formatInteger(n: NumberLike): string {
  return numeral(n).format('0,0');
}

export function formatNumber(n: NumberLike): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 20 });
}

export function formatBytes(n: NumberLike): string {
  return numeral(n).format('0.00 b');
}

export function formatBytesCompact(n: NumberLike): string {
  return numeral(n).format('0.00b');
}

export function formatMegabytes(n: NumberLike): string {
  return numeral(Number(n) / 1048576).format('0,0.0');
}

export function formatPercent(n: NumberLike): string {
  return (Number(n) * 100).toFixed(2) + '%';
}

export function formatPercentClapped(n: NumberLike): string {
  return formatPercent(Math.min(Math.max(Number(n), 0), 1));
}

export function formatMillions(n: NumberLike): string {
  const s = (Number(n) / 1e6).toFixed(3);
  if (s === '0.000') return String(Math.round(Number(n)));
  return s + ' M';
}

function pad2(str: string | number): string {
  return ('00' + str).substr(-2);
}

function pad3(str: string | number): string {
  return ('000' + str).substr(-3);
}

export function formatDuration(ms: NumberLike): string {
  const n = Number(ms);
  const timeInHours = Math.floor(n / 3600000);
  const timeInMin = Math.floor(n / 60000) % 60;
  const timeInSec = Math.floor(n / 1000) % 60;
  return timeInHours + ':' + pad2(timeInMin) + ':' + pad2(timeInSec);
}

export function formatDurationWithMs(ms: NumberLike): string {
  const n = Number(ms);
  const timeInHours = Math.floor(n / 3600000);
  const timeInMin = Math.floor(n / 60000) % 60;
  const timeInSec = Math.floor(n / 1000) % 60;
  return (
    timeInHours + ':' + pad2(timeInMin) + ':' + pad2(timeInSec) + '.' + pad3(Math.floor(n) % 1000)
  );
}

export function pluralIfNeeded(n: NumberLike, singular: string, plural?: string): string {
  if (!plural) plural = singular + 's';
  return `${formatInteger(n)} ${n === 1 ? singular : plural}`;
}

// ----------------------------

export function parseJson(json: string): any {
  try {
    return JSONBig.parse(json);
  } catch (e) {
    return undefined;
  }
}

export function validJson(json: string): boolean {
  try {
    JSONBig.parse(json);
    return true;
  } catch (e) {
    return false;
  }
}

export function filterMap<T, Q>(xs: readonly T[], f: (x: T, i: number) => Q | undefined): Q[] {
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

export function arrangeWithPrefixSuffix(
  things: readonly string[],
  prefix: readonly string[],
  suffix: readonly string[],
): string[] {
  const pre = uniq(prefix.filter(x => things.includes(x)));
  const mid = things.filter(x => !prefix.includes(x) && !suffix.includes(x));
  const post = uniq(suffix.filter(x => things.includes(x)));
  return pre.concat(mid, post);
}

// ----------------------------

export function downloadFile(text: string, type: string, filename: string): void {
  let blobType;
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

export function swapElements<T>(items: readonly T[], indexA: number, indexB: number): T[] {
  const newItems = items.concat();
  const t = newItems[indexA];
  newItems[indexA] = newItems[indexB];
  newItems[indexB] = t;
  return newItems;
}

export function moveElement<T>(items: readonly T[], fromIndex: number, toIndex: number): T[] {
  const indexDiff = fromIndex - toIndex;
  if (indexDiff > 0) {
    // move left
    return [
      ...items.slice(0, toIndex),
      items[fromIndex],
      ...items.slice(toIndex, fromIndex),
      ...items.slice(fromIndex + 1, items.length),
    ];
  } else if (indexDiff < 0) {
    // move right
    const targetIndex = toIndex + 1;
    return [
      ...items.slice(0, fromIndex),
      ...items.slice(fromIndex + 1, targetIndex),
      items[fromIndex],
      ...items.slice(targetIndex, items.length),
    ];
  } else {
    // do nothing
    return items.slice();
  }
}

export function moveToIndex<T>(
  items: readonly T[],
  itemToIndex: (item: T, i: number) => number,
): T[] {
  const frontItems: { item: T; index: number }[] = [];
  const otherItems: T[] = [];
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    const index = itemToIndex(item, i);
    if (index >= 0) {
      frontItems.push({ item, index });
    } else {
      otherItems.push(item);
    }
  }

  return frontItems
    .sort((a, b) => a.index - b.index)
    .map(d => d.item)
    .concat(otherItems);
}

export function stringifyValue(value: unknown): string {
  switch (typeof value) {
    case 'object':
      if (!value) return String(value);
      if (typeof (value as any).toISOString === 'function') return (value as any).toISOString();
      return JSONBig.stringify(value);

    default:
      return String(value);
  }
}

export function isInBackground(): boolean {
  return document.visibilityState === 'hidden';
}
