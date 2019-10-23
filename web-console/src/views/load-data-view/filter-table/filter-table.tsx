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
import { caseInsensitiveContains, filterMap } from '../../../utils';
import { DruidFilter } from '../../../utils/ingestion-spec';
import { HeaderAndRows, SampleEntry } from '../../../utils/sampler';

import './filter-table.scss';

export interface FilterTableProps {
  sampleData: HeaderAndRows;
  columnFilter: string;
  dimensionFilters: DruidFilter[];
  selectedFilterIndex: number;
  onShowGlobalFilter: () => void;
  onFilterSelect: (filter: DruidFilter, index: number) => void;
}

export function FilterTable(props: FilterTableProps) {
  const {
    sampleData,
    columnFilter,
    dimensionFilters,
    selectedFilterIndex,
    onShowGlobalFilter,
    onFilterSelect,
  } = props;

  return (
    <ReactTable
      className="filter-table -striped -highlight"
      data={sampleData.rows}
      columns={filterMap(sampleData.header, (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;
        const timestamp = columnName === '__time';
        const filterIndex = dimensionFilters.findIndex(f => f.dimension === columnName);
        const filter = dimensionFilters[filterIndex];

        const columnClassName = classNames({
          filtered: filter,
          selected: filter && filterIndex === selectedFilterIndex,
        });
        return {
          Header: (
            <div
              className={classNames('clickable')}
              onClick={() => {
                if (timestamp) {
                  onShowGlobalFilter();
                } else if (filter) {
                  onFilterSelect(filter, filterIndex);
                } else {
                  onFilterSelect({ type: 'selector', dimension: columnName, value: '' }, -1);
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
          Cell: row => <TableCell value={row.value} timestamp={timestamp} />,
        };
      })}
      defaultPageSize={50}
      showPagination={false}
      sortable={false}
    />
  );
}
