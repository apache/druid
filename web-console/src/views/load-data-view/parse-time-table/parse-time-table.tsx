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
import { TableCellUnparseable } from '../../../components/table-cell-unparseable/table-cell-unparseable';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import { possibleDruidFormatForValues } from '../../../utils/druid-time';
import {
  getTimestampSpecColumn,
  isColumnTimestampSpec,
  TimestampSpec,
} from '../../../utils/ingestion-spec';
import { HeaderAndRows, SampleEntry } from '../../../utils/sampler';

import './parse-time-table.scss';

export function parseTimeTableSelectedColumnName(
  sampleData: HeaderAndRows,
  timestampSpec: TimestampSpec | undefined,
): string | undefined {
  if (!timestampSpec) return;
  const timestampColumn = timestampSpec.column;
  if (!timestampColumn || !sampleData.header.includes(timestampColumn)) return;
  return timestampColumn;
}

export interface ParseTimeTableProps {
  sampleBundle: {
    headerAndRows: HeaderAndRows;
    timestampSpec: TimestampSpec;
  };
  columnFilter: string;
  possibleTimestampColumnsOnly: boolean;
  selectedColumnName: string | undefined;
  onTimestampColumnSelect: (newTimestampSpec: TimestampSpec) => void;
}

export const ParseTimeTable = React.memo(function ParseTimeTable(props: ParseTimeTableProps) {
  const {
    sampleBundle,
    columnFilter,
    possibleTimestampColumnsOnly,
    selectedColumnName,
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
          const used = timestampSpec.column === columnName;
          const possibleFormat = timestamp
            ? null
            : possibleDruidFormatForValues(
                filterMap(headerAndRows.rows, d => (d.parsed ? d.parsed[columnName] : undefined)),
              );
          if (possibleTimestampColumnsOnly && !timestamp && !possibleFormat) return;

          const columnClassName = classNames({
            timestamp,
            used,
            selected: selectedColumnName === columnName,
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
                return <TableCellUnparseable timestamp={timestamp} />;
              }
              return <TableCell value={timestamp ? new Date(row.value) : row.value} />;
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
});
