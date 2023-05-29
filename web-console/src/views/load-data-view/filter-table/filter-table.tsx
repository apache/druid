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
import type { RowRenderProps } from 'react-table';
import ReactTable from 'react-table';

import { TableCell } from '../../../components';
import type { DruidFilter } from '../../../druid-models';
import { getFilterDimension, TIME_COLUMN } from '../../../druid-models';
import {
  DEFAULT_TABLE_CLASS_NAME,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../../react-table';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import type { SampleEntry, SampleResponse } from '../../../utils/sampler';
import { getHeaderNamesFromSampleResponse } from '../../../utils/sampler';

import './filter-table.scss';

export function filterTableSelectedColumnName(
  sampleResponse: SampleResponse,
  selectedFilter: Partial<DruidFilter> | undefined,
): string | undefined {
  if (!selectedFilter) return;
  const selectedFilterName = selectedFilter.dimension;
  if (!getHeaderNamesFromSampleResponse(sampleResponse).includes(selectedFilterName)) return;
  return selectedFilterName;
}

export interface FilterTableProps {
  sampleResponse: SampleResponse;
  columnFilter: string;
  dimensionFilters: DruidFilter[];
  selectedFilterName: string | undefined;
  onFilterSelect: (filter: DruidFilter, index: number) => void;
}

export const FilterTable = React.memo(function FilterTable(props: FilterTableProps) {
  const { sampleResponse, columnFilter, dimensionFilters, selectedFilterName, onFilterSelect } =
    props;

  return (
    <ReactTable
      className={classNames('filter-table', DEFAULT_TABLE_CLASS_NAME)}
      data={sampleResponse.data}
      sortable={false}
      defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
      pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
      showPagination={sampleResponse.data.length > STANDARD_TABLE_PAGE_SIZE}
      columns={filterMap(getHeaderNamesFromSampleResponse(sampleResponse), (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;
        const isTimestamp = columnName === TIME_COLUMN;
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
                    isTimestamp
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
          Cell: function FilterTableCell(row: RowRenderProps) {
            return <TableCell value={isTimestamp ? new Date(Number(row.value)) : row.value} />;
          },
        };
      })}
    />
  );
});
