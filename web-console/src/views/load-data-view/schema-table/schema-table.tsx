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
import {
  DimensionSpec,
  getDimensionSpecName,
  getDimensionSpecType,
  getMetricSpecName,
  inflateDimensionSpec,
  MetricSpec,
} from '../../../druid-models';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import { HeaderAndRows, SampleEntry } from '../../../utils/sampler';

import './schema-table.scss';

export interface SchemaTableProps {
  sampleBundle: {
    headerAndRows: HeaderAndRows;
    dimensions: (string | DimensionSpec)[] | undefined;
    metricsSpec: MetricSpec[] | undefined;
  };
  columnFilter: string;
  selectedAutoDimension: string | undefined;
  selectedDimensionSpecIndex: number;
  selectedMetricSpecIndex: number;
  onAutoDimensionSelect: (dimensionName: string) => void;
  onDimensionSelect: (
    selectedDimensionSpec: DimensionSpec | undefined,
    selectedDimensionSpecIndex: number,
  ) => void;
  onMetricSelect: (
    selectedMetricSpec: MetricSpec | undefined,
    selectedMetricSpecIndex: number,
  ) => void;
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
  const { headerAndRows, dimensions, metricsSpec } = sampleBundle;

  return (
    <ReactTable
      className="schema-table -striped -highlight"
      data={headerAndRows.rows}
      columns={filterMap(headerAndRows.header, (columnName, i) => {
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
            Cell: ({ value }) => <TableCell value={value} />,
          };
        } else {
          const isTimestamp = columnName === '__time';
          const dimensionSpecIndex = dimensions
            ? dimensions.findIndex(d => getDimensionSpecName(d) === columnName)
            : -1;
          const dimensionSpec = dimensions ? dimensions[dimensionSpecIndex] : undefined;
          const dimensionSpecType = dimensionSpec ? getDimensionSpecType(dimensionSpec) : undefined;

          const columnClassName = classNames(
            isTimestamp ? 'timestamp' : 'dimension',
            dimensionSpecType || 'string',
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

                  if (dimensionSpec) {
                    onDimensionSelect(inflateDimensionSpec(dimensionSpec), dimensionSpecIndex);
                  } else {
                    onAutoDimensionSelect(columnName);
                  }
                }}
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">
                  {isTimestamp ? 'long (time column)' : dimensionSpecType || 'string (auto)'}&nbsp;
                </div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            width: isTimestamp ? 200 : 100,
            accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
            Cell: row => <TableCell value={isTimestamp ? new Date(row.value) : row.value} />,
          };
        }
      })}
      defaultPageSize={50}
      showPagination={false}
      sortable={false}
    />
  );
});
