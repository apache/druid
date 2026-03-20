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

import { SqlExpression } from 'druid-query-toolkit';
import type { Filter } from 'react-table';

import { addOrUpdate, filterMap } from '../index';

import { TableFilter } from './table-filter';

export class TableFilters {
  private readonly filters: readonly TableFilter[];

  static fromString(str: string | undefined): TableFilters {
    if (!str) return new TableFilters([]);
    const filters = filterMap(str.split('&'), clause => {
      return TableFilter.fromSingleTableFilterString(clause);
    });
    return new TableFilters(filters);
  }

  static fromFilters(filters: Filter[]): TableFilters {
    return new TableFilters(filters.map(TableFilter.fromFilter));
  }

  static eq(keyValue: Record<string, string>): TableFilters {
    const filters = Object.entries(keyValue).map(
      ([key, value]) => new TableFilter(key, '=', value),
    );
    return new TableFilters(filters);
  }

  static empty(): TableFilters {
    return new TableFilters([]);
  }

  constructor(filters?: TableFilter | TableFilter[] | TableFilters) {
    if (!filters) {
      this.filters = [];
    } else if (filters instanceof TableFilters) {
      this.filters = filters.filters;
    } else if (Array.isArray(filters)) {
      this.filters = filters;
    } else {
      this.filters = [filters];
    }
  }

  toString(): string {
    return this.filters
      .map(
        filter =>
          `${filter.key}${filter.mode}${filter.values
            .join('|')
            .replace(/[&%/]/g, encodeURIComponent)}`,
      )
      .join('&');
  }

  toFilters(): Filter[] {
    return this.filters.map(f => f.toFilter());
  }

  addOrUpdate(filter: TableFilter): TableFilters {
    return new TableFilters(addOrUpdate(this.filters, filter, f => f.key));
  }

  toSqlExpression(): SqlExpression {
    return SqlExpression.and(
      ...filterMap(this.filters, function (filter: TableFilter): SqlExpression | undefined {
        return filter.toSqlExpression();
      }),
    );
  }

  toArray(): readonly TableFilter[] {
    return this.filters;
  }

  isEmpty(): boolean {
    return this.filters.length === 0;
  }

  size(): number {
    return this.filters.length;
  }

  equals(other: TableFilters): boolean {
    if (this.filters.length !== other.filters.length) return false;
    return this.filters.every((f, i) => f.equals(other.filters[i]));
  }
}
