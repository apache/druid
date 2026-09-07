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

import type { IconName } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { C, F, SqlExpression } from 'druid-query-toolkit';
import type { Filter } from 'react-table';

import { caseInsensitiveContains } from '../index';

export type FilterMode = '~' | '=' | '!=' | '<' | '<=' | '>' | '>=';

export class TableFilter {
  static readonly MODES: FilterMode[] = ['~', '=', '!=', '<', '<=', '>', '>='];
  static readonly MODES_NO_COMPARISON: FilterMode[] = ['~', '=', '!='];

  public readonly key: string;
  public readonly mode: FilterMode;
  public readonly values: string[];

  static fromSingleTableFilterString(str: string): TableFilter | undefined {
    const m = /^(\w+)((?:~|=|!=|<(?!=)|<=|>(?!=)|>=).*)$/.exec(
      str.replace(/%2[56F]/g, decodeURIComponent),
    );
    if (!m) return;

    const key = m[1];
    const modeAndValue = m[2];

    // Parse mode from the value
    const modeMatch = /^(~|=|!=|<(?!=)|<=|>(?!=)|>=)?(.*)$/.exec(modeAndValue);
    if (!modeMatch) return;

    const mode = (modeMatch[1] as FilterMode) || '~';
    const value = modeMatch[2] || '';

    return new TableFilter(key, mode, value);
  }

  static fromFilter(filter: Filter): TableFilter {
    const modeMatch = /^(~|=|!=|<(?!=)|<=|>(?!=)|>=)?(.*)$/.exec(String(filter.value));
    if (!modeMatch) {
      return new TableFilter(filter.id, '~', String(filter.value));
    }

    const mode = (modeMatch[1] as FilterMode) || '~';
    const value = modeMatch[2] || '';

    return new TableFilter(filter.id, mode, value);
  }

  constructor(key: string, mode: FilterMode, value: string | string[]) {
    this.key = key;
    this.mode = mode;
    this.values = typeof value === 'string' ? value.split('|') : value;
  }

  public get value(): string {
    return this.values.join('|');
  }

  public toFilter(): Filter {
    return {
      id: this.key,
      value: `${this.mode}${this.value}`,
    };
  }

  public equals(other: TableFilter): boolean {
    return (
      this.key === other.key &&
      this.mode === other.mode &&
      this.values.length === other.values.length &&
      this.values.every((v, i) => v === other.values[i])
    );
  }

  public matches(value: unknown): boolean {
    if (value == null) return false;
    const strValue = String(value);

    switch (this.mode) {
      case '=':
        return this.values.some(needle => strValue === needle);

      case '!=':
        return this.values.every(needle => strValue !== needle);

      case '<':
        return this.values.some(needle => strValue < needle);

      case '<=':
        return this.values.some(needle => strValue <= needle);

      case '>':
        return this.values.some(needle => strValue > needle);

      case '>=':
        return this.values.some(needle => strValue >= needle);

      default:
        return this.values.some(needle => caseInsensitiveContains(strValue, needle));
    }
  }

  public toSqlExpression(): SqlExpression {
    const column = C(this.key);

    switch (this.mode) {
      case '=': {
        return SqlExpression.or(...this.values.map(needle => column.equal(needle)));
      }

      case '!=': {
        return SqlExpression.and(...this.values.map(needle => column.unequal(needle)));
      }

      case '<':
        return SqlExpression.or(...this.values.map(needle => column.lessThan(needle)));

      case '<=':
        return SqlExpression.or(...this.values.map(needle => column.lessThanOrEqual(needle)));

      case '>':
        return SqlExpression.or(...this.values.map(needle => column.greaterThan(needle)));

      case '>=':
        return SqlExpression.or(...this.values.map(needle => column.greaterThanOrEqual(needle)));

      default:
        return SqlExpression.or(
          ...this.values.map(needle => F('LOWER', column).like(`%${needle.toLowerCase()}%`)),
        );
    }
  }

  static modeToIcon(mode: FilterMode): IconName {
    switch (mode) {
      case '~':
        return IconNames.SEARCH;
      case '=':
        return IconNames.EQUALS;
      case '!=':
        return IconNames.NOT_EQUAL_TO;
      case '<':
        return IconNames.LESS_THAN;
      case '<=':
        return IconNames.LESS_THAN_OR_EQUAL_TO;
      case '>':
        return IconNames.GREATER_THAN;
      case '>=':
        return IconNames.GREATER_THAN_OR_EQUAL_TO;
      default:
        return IconNames.BLANK;
    }
  }

  static modeToTitle(mode: FilterMode): string {
    switch (mode) {
      case '~':
        return 'Search';
      case '=':
        return 'Equals';
      case '!=':
        return 'Not equals';
      case '<':
        return 'Less than';
      case '<=':
        return 'Less than or equal';
      case '>':
        return 'Greater than';
      case '>=':
        return 'Greater than or equal';
      default:
        return '?';
    }
  }

  static combineModeAndNeedle(mode: FilterMode, needle: string, cleanup = false): string {
    if (cleanup && needle === '') return '';
    return `${mode}${needle}`;
  }

  static parseModeAndNeedle(
    filter: Filter,
    loose = false,
  ): { mode: FilterMode; needle: string; needleParts: string[] } | undefined {
    const m = /^(~|=|!=|<(?!=)|<=|>(?!=)|>=)?(.*)$/.exec(String(filter.value));
    if (!m) return;
    if (!loose && !m[2]) return;
    const mode = (m[1] as FilterMode) || '~';
    const needle = m[2] || '';
    return {
      mode,
      needle,
      needleParts: needle.split('|'),
    };
  }
}
