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
import type { Transform } from '../../../druid-models';
import { TIME_COLUMN } from '../../../druid-models';
import {
  DEFAULT_TABLE_CLASS_NAME,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../../react-table';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import { escapeColumnName } from '../../../utils/druid-expression';
import type { SampleEntry, SampleResponse } from '../../../utils/sampler';
import { getHeaderNamesFromSampleResponse } from '../../../utils/sampler';

import './transform-table.scss';

export function transformTableSelectedColumnName(
  sampleResponse: SampleResponse,
  selectedTransform: Partial<Transform> | undefined,
): string | undefined {
  if (!selectedTransform) return;
  const selectedTransformName = selectedTransform.name;
  if (
    selectedTransformName &&
    !getHeaderNamesFromSampleResponse(sampleResponse).includes(selectedTransformName)
  ) {
    return;
  }
  return selectedTransformName;
}

export interface TransformTableProps {
  sampleResponse: SampleResponse;
  columnFilter: string;
  transformedColumnsOnly: boolean;
  transforms: Transform[];
  selectedColumnName: string | undefined;
  onTransformSelect: (transform: Transform, index: number) => void;
}

export const TransformTable = React.memo(function TransformTable(props: TransformTableProps) {
  const {
    sampleResponse,
    columnFilter,
    transformedColumnsOnly,
    transforms,
    selectedColumnName,
    onTransformSelect,
  } = props;

  return (
    <ReactTable
      className={classNames('transform-table', DEFAULT_TABLE_CLASS_NAME)}
      data={sampleResponse.data}
      sortable={false}
      defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
      pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
      showPagination={sampleResponse.data.length > STANDARD_TABLE_PAGE_SIZE}
      columns={filterMap(getHeaderNamesFromSampleResponse(sampleResponse), (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;
        const isTimestamp = columnName === TIME_COLUMN;
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
          width: 140,
          Cell: function TransformTableCell(row: RowRenderProps) {
            return <TableCell value={isTimestamp ? new Date(Number(row.value)) : row.value} />;
          },
        };
      })}
    />
  );
});
