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
import { possibleDruidFormatForValues } from '../../../utils/druid-time';
import {
  getTimestampSpecColumn,
  isColumnTimestampSpec,
  TimestampSpec,
} from '../../../utils/ingestion-spec';
import { HeaderAndRows, SampleEntry } from '../../../utils/sampler';

import './parse-time-table.scss';

export interface ParseTimeTableProps {
  sampleBundle: {
    headerAndRows: HeaderAndRows;
    timestampSpec: TimestampSpec;
  };
  columnFilter: string;
  possibleTimestampColumnsOnly: boolean;
  onTimestampColumnSelect: (newTimestampSpec: TimestampSpec) => void;
}

export function ParseTimeTable(props: ParseTimeTableProps) {
  const {
    sampleBundle,
    columnFilter,
    possibleTimestampColumnsOnly,
    onTimestampColumnSelect,
  } = props;
  const { headerAndRows, timestampSpec } = sampleBundle;
  const timestampSpecColumn = getTimestampSpecColumn(timestampSpec);
  const timestampSpecFromColumn = isColumnTimestampSpec(timestampSpec);

  return (
    <ReactTable
      className="parse-time-table -striped -highlight"
      data={headerAndRows.rows}
      columns={filterMap(
        headerAndRows.header.length ? headerAndRows.header : ['__error__'],
        (columnName, i) => {
          const timestamp = columnName === '__time';
          if (!timestamp && !caseInsensitiveContains(columnName, columnFilter)) return;
          const selected = timestampSpec.column === columnName;
          const possibleFormat = timestamp
            ? null
            : possibleDruidFormatForValues(
                filterMap(headerAndRows.rows, d => (d.parsed ? d.parsed[columnName] : undefined)),
              );
          if (possibleTimestampColumnsOnly && !timestamp && !possibleFormat) return;

          const columnClassName = classNames({
            timestamp,
            selected,
          });
          return {
            Header: (
              <div
                className={classNames({ clickable: !timestamp })}
                onClick={
                  timestamp
                    ? undefined
                    : () => {
                        onTimestampColumnSelect({
                          column: columnName,
                          format: possibleFormat || '!!! Could not auto detect a format !!!',
                        });
                      }
                }
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">
                  {timestamp
                    ? timestampSpecFromColumn
                      ? `from: '${timestampSpecColumn}'`
                      : `mv: ${timestampSpec.missingValue}`
                    : possibleFormat || ''}
                  &nbsp;
                </div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
            Cell: row => {
              if (columnName === '__error__') {
                return <TableCell value={row.original.error} />;
              }
              if (row.original.unparseable) {
                return <TableCell unparseable />;
              }
              return <TableCell value={row.value} timestamp={timestamp} />;
            },
            minWidth: timestamp ? 200 : 100,
            resizable: !timestamp,
          };
        },
      )}
      defaultPageSize={50}
      showPagination={false}
      sortable={false}
    />
  );
}
