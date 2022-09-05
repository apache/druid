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

import classNames from 'classnames';
import React from 'react';
import ReactTable from 'react-table';

import { TableCell } from '../../../components';
import { DruidFilter, getFilterDimension } from '../../../druid-models';
import {
  DEFAULT_TABLE_CLASS_NAME,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../../react-table';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import { SampleEntry, SampleHeaderAndRows } from '../../../utils/sampler';

import './filter-table.scss';

export function filterTableSelectedColumnName(
  sampleData: SampleHeaderAndRows,
  selectedFilter: Partial<DruidFilter> | undefined,
): string | undefined {
  if (!selectedFilter) return;
  const selectedFilterName = selectedFilter.dimension;
  if (!sampleData.header.includes(selectedFilterName)) return;
  return selectedFilterName;
}

export interface FilterTableProps {
  sampleData: SampleHeaderAndRows;
  columnFilter: string;
  dimensionFilters: DruidFilter[];
  selectedFilterName: string | undefined;
  onFilterSelect: (filter: DruidFilter, index: number) => void;
}

export const FilterTable = React.memo(function FilterTable(props: FilterTableProps) {
  const { sampleData, columnFilter, dimensionFilters, selectedFilterName, onFilterSelect } = props;

  return (
    <ReactTable
      className={classNames('filter-table', DEFAULT_TABLE_CLASS_NAME)}
      data={sampleData.rows}
      sortable={false}
      defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
      pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
      showPagination={sampleData.rows.length > STANDARD_TABLE_PAGE_SIZE}
      columns={filterMap(sampleData.header, (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;
        const timestamp = columnName === '__time';
        const filterIndex = dimensionFilters.findIndex(f => getFilterDimension(f) === columnName);
        const filter = dimensionFilters[filterIndex];

        const columnClassName = classNames({
          filtered: filter,
          selected: columnName === selectedFilterName,
        });
        return {
          Header: (
            <div
              className="clickable"
              onClick={() => {
                if (filter) {
                  onFilterSelect(filter, filterIndex);
                } else {
                  onFilterSelect(
                    timestamp
                      ? { type: 'interval', dimension: columnName, intervals: [] }
                      : { type: 'selector', dimension: columnName, value: '' },
                    -1,
                  );
                }
              }}
            >
              <div className="column-name">{columnName}</div>
              <div className="column-detail">{filter ? `(filtered)` : ''}&nbsp;</div>
            </div>
          ),
          headerClassName: columnClassName,
          className: columnClassName,
          id: String(i),
          accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
          width: 140,
          Cell: function FilterTableCell(row) {
            return <TableCell value={timestamp ? new Date(row.value) : row.value} />;
          },
        };
      })}
    />
  );
});
