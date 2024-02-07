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
import type { DimensionSpec, MetricSpec } from '../../../druid-models';
import {
  getDimensionSpecClassType,
  getDimensionSpecName,
  getDimensionSpecUserType,
  getMetricSpecName,
  inflateDimensionSpec,
  TIME_COLUMN,
} from '../../../druid-models';
import {
  DEFAULT_TABLE_CLASS_NAME,
  STANDARD_TABLE_PAGE_SIZE,
  STANDARD_TABLE_PAGE_SIZE_OPTIONS,
} from '../../../react-table';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import type { SampleEntry, SampleResponse } from '../../../utils/sampler';
import { getHeaderNamesFromSampleResponse } from '../../../utils/sampler';

import './schema-table.scss';

export interface SchemaTableProps {
  sampleBundle: {
    sampleResponse: SampleResponse;
    dimensions: (string | DimensionSpec)[] | undefined;
    metricsSpec: MetricSpec[] | undefined;
    definedDimensions: boolean;
  };
  columnFilter: string;
  selectedAutoDimension: string | undefined;
  selectedDimensionSpecIndex: number;
  selectedMetricSpecIndex: number;
  onAutoDimensionSelect: (dimensionName: string) => void;
  onDimensionSelect: (dimensionSpec: DimensionSpec, index: number) => void;
  onMetricSelect: (metricSpec: MetricSpec, index: number) => void;
}

export const SchemaTable = React.memo(function SchemaTable(props: SchemaTableProps) {
  const {
    sampleBundle,
    columnFilter,
    selectedAutoDimension,
    selectedDimensionSpecIndex,
    selectedMetricSpecIndex,
    onAutoDimensionSelect,
    onDimensionSelect,
    onMetricSelect,
  } = props;
  const { sampleResponse, dimensions, metricsSpec, definedDimensions } = sampleBundle;

  return (
    <ReactTable
      className={classNames('schema-table', DEFAULT_TABLE_CLASS_NAME)}
      data={sampleResponse.data}
      sortable={false}
      defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
      pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
      showPagination={sampleResponse.data.length > STANDARD_TABLE_PAGE_SIZE}
      columns={filterMap(getHeaderNamesFromSampleResponse(sampleResponse), (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;

        const metricSpecIndex = metricsSpec
          ? metricsSpec.findIndex(m => getMetricSpecName(m) === columnName)
          : -1;
        const metricSpec = metricsSpec ? metricsSpec[metricSpecIndex] : undefined;

        if (metricSpec) {
          const columnClassName = classNames('metric', {
            selected: metricSpec && metricSpecIndex === selectedMetricSpecIndex,
          });
          return {
            Header: (
              <div
                className="clickable"
                onClick={() => onMetricSelect(metricSpec, metricSpecIndex)}
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">{metricSpec.type}&nbsp;</div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
            width: 120,
            Cell: function SchemaTableCell({ value }: RowRenderProps) {
              return <TableCell value={value} />;
            },
          };
        } else {
          const isTimestamp = columnName === TIME_COLUMN;
          const dimensionSpecIndex = dimensions
            ? dimensions.findIndex(d => getDimensionSpecName(d) === columnName)
            : -1;
          const dimensionSpec = dimensions ? dimensions[dimensionSpecIndex] : undefined;
          const dimensionSpecUserType = dimensionSpec
            ? getDimensionSpecUserType(dimensionSpec, definedDimensions)
            : undefined;
          const dimensionSpecClassType = dimensionSpec
            ? getDimensionSpecClassType(dimensionSpec, definedDimensions)
            : undefined;

          const columnClassName = classNames(
            isTimestamp ? 'timestamp' : 'dimension',
            dimensionSpecClassType || 'string',
            {
              selected:
                (dimensionSpec && dimensionSpecIndex === selectedDimensionSpecIndex) ||
                selectedAutoDimension === columnName,
            },
          );
          return {
            Header: (
              <div
                className="clickable"
                onClick={() => {
                  if (isTimestamp) return;

                  if (definedDimensions && dimensionSpec) {
                    onDimensionSelect(inflateDimensionSpec(dimensionSpec), dimensionSpecIndex);
                  } else {
                    onAutoDimensionSelect(columnName);
                  }
                }}
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">
                  {isTimestamp ? 'long (time column)' : dimensionSpecUserType || '(auto)'}&nbsp;
                </div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            width: isTimestamp ? 200 : 140,
            accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
            Cell: function SchemaTableCell(row: RowRenderProps) {
              return <TableCell value={isTimestamp ? new Date(Number(row.value)) : row.value} />;
            },
          };
        }
      })}
    />
  );
});
