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
import { escapeColumnName } from '../../../utils/druid-expression';
import { Transform } from '../../../utils/ingestion-spec';
import { HeaderAndRows, SampleEntry } from '../../../utils/sampler';

import './transform-table.scss';

export function transformTableSelectedColumnName(
  sampleData: HeaderAndRows,
  selectedTransform: Transform | undefined,
): string | undefined {
  if (!selectedTransform) return;
  const selectedTransformName = selectedTransform.name;
  if (!sampleData.header.includes(selectedTransformName)) return;
  return selectedTransformName;
}

export interface TransformTableProps {
  sampleData: HeaderAndRows;
  columnFilter: string;
  transformedColumnsOnly: boolean;
  transforms: Transform[];
  selectedColumnName: string | undefined;
  onTransformSelect: (transform: Transform, index: number) => void;
}

export const TransformTable = React.memo(function TransformTable(props: TransformTableProps) {
  const {
    sampleData,
    columnFilter,
    transformedColumnsOnly,
    transforms,
    selectedColumnName,
    onTransformSelect,
  } = props;

  return (
    <ReactTable
      className="transform-table -striped -highlight"
      data={sampleData.rows}
      columns={filterMap(sampleData.header, (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;
        const timestamp = columnName === '__time';
        const transformIndex = transforms.findIndex(f => f.name === columnName);
        if (transformIndex === -1 && transformedColumnsOnly) return;
        const transform = transforms[transformIndex];

        const columnClassName = classNames({
          transformed: transform,
          selected: columnName === selectedColumnName,
        });
        return {
          Header: (
            <div
              className={classNames('clickable')}
              onClick={() => {
                if (transform) {
                  onTransformSelect(transform, transformIndex);
                } else {
                  onTransformSelect(
                    {
                      type: 'expression',
                      name: columnName,
                      expression: escapeColumnName(columnName),
                    },
                    transformIndex,
                  );
                }
              }}
            >
              <div className="column-name">{columnName}</div>
              <div className="column-detail">
                {transform ? `= ${transform.expression}` : ''}&nbsp;
              </div>
            </div>
          ),
          headerClassName: columnClassName,
          className: columnClassName,
          id: String(i),
          accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
          Cell: row => <TableCell value={timestamp ? new Date(row.value) : row.value} />,
        };
      })}
      defaultPageSize={50}
      showPagination={false}
      sortable={false}
    />
  );
});
