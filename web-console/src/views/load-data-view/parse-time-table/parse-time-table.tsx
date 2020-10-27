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
import {
  getTimestampDetailFromSpec,
  getTimestampSpecColumnFromSpec,
  IngestionSpec,
  possibleDruidFormatForValues,
  TimestampSpec,
} from '../../../druid-models';
import { caseInsensitiveContains, filterMap } from '../../../utils';
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
    spec: IngestionSpec;
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
  const { headerAndRows, spec } = sampleBundle;
  const timestampSpecColumn = getTimestampSpecColumnFromSpec(spec);
  const timestampDetail = getTimestampDetailFromSpec(spec);

  return (
    <ReactTable
      className="parse-time-table -striped -highlight"
      data={headerAndRows.rows}
      columns={filterMap(
        headerAndRows.header.length ? headerAndRows.header : ['__error__'],
        (columnName, i) => {
          const isTimestamp = columnName === '__time';
          if (!isTimestamp && !caseInsensitiveContains(columnName, columnFilter)) return;
          const used = timestampSpecColumn === columnName;
          const possibleFormat = isTimestamp
            ? null
            : possibleDruidFormatForValues(
                filterMap(headerAndRows.rows, d => (d.parsed ? d.parsed[columnName] : undefined)),
              );
          if (possibleTimestampColumnsOnly && !isTimestamp && !possibleFormat) return;

          const columnClassName = classNames({
            timestamp: isTimestamp,
            used,
            selected: selectedColumnName === columnName,
          });
          return {
            Header: (
              <div
                className={classNames({ clickable: !isTimestamp })}
                onClick={
                  isTimestamp
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
                  {isTimestamp ? timestampDetail : possibleFormat || ''}
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
                return <TableCellUnparseable timestamp={isTimestamp} />;
              }
              return <TableCell value={isTimestamp ? new Date(row.value) : row.value} />;
            },
            minWidth: isTimestamp ? 200 : 100,
            resizable: !isTimestamp,
          };
        },
      )}
      defaultPageSize={50}
      showPagination={false}
      sortable={false}
    />
  );
});
