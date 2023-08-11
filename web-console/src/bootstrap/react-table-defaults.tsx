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

import React from 'react';
import type { Filter } from 'react-table';
import { ReactTableDefaults } from 'react-table';

import { Loader } from '../components';
import {
  booleanCustomTableFilter,
  DEFAULT_TABLE_CLASS_NAME,
  GenericFilterInput,
  ReactTablePagination,
} from '../react-table';
import { countBy } from '../utils';

const NoData = React.memo(function NoData(props: { children?: React.ReactNode }) {
  const { children } = props;
  if (!children) return null;
  return <div className="rt-noData">{children}</div>;
});

export function bootstrapReactTable() {
  Object.assign(ReactTableDefaults, {
    className: DEFAULT_TABLE_CLASS_NAME,
    defaultFilterMethod: (filter: Filter, row: any) => {
      const id = filter.pivotId || filter.id;
      const subRows = row._subRows;
      if (Array.isArray(subRows)) {
        return subRows.some(r => booleanCustomTableFilter(filter, r[id]));
      } else {
        return booleanCustomTableFilter(filter, row[id]);
      }
    },
    LoadingComponent: Loader,
    loadingText: '',
    NoDataComponent: NoData,
    FilterComponent: GenericFilterInput,
    PaginationComponent: ReactTablePagination,
    PivotValueComponent: function PivotValue(opt: any) {
      const { value, subRows } = opt;
      let msg = String(value);
      if (msg === 'undefined') msg = 'n/a';
      if (subRows) {
        msg += ` (${subRows.length})`;
      }
      return <span className="default-pivoted">{msg}</span>;
    },
    AggregatedComponent: function Aggregated(opt: any) {
      const { subRows, column } = opt;
      const previewValues = subRows
        .filter((d: any) => typeof d[column.id] !== 'undefined')
        .map((row: any) => row[column.id]);
      const previewCount = countBy(previewValues);
      return (
        <div className="default-aggregated">
          {Object.keys(previewCount)
            .sort()
            .map(v => `${v} (${previewCount[v]})`)
            .join(', ')}
        </div>
      );
    },
    defaultPageSize: 20,
  });
}
