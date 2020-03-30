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
import { caseInsensitiveContains, filterMap, sortWithPrefixSuffix } from '../../../utils';
import {
  DimensionSpec,
  DimensionsSpec,
  getDimensionSpecName,
  getDimensionSpecType,
  getMetricSpecName,
  inflateDimensionSpec,
  MetricSpec,
} from '../../../utils/ingestion-spec';
import { HeaderAndRows, SampleEntry } from '../../../utils/sampler';

import './schema-table.scss';

export interface SchemaTableProps {
  sampleBundle: {
    headerAndRows: HeaderAndRows;
    dimensionsSpec: DimensionsSpec;
    metricsSpec: MetricSpec[];
  };
  columnFilter: string;
  selectedDimensionSpecIndex: number;
  selectedMetricSpecIndex: number;
  onDimensionOrMetricSelect: (
    selectedDimensionSpec: DimensionSpec | undefined,
    selectedDimensionSpecIndex: number,
    selectedMetricSpec: MetricSpec | undefined,
    selectedMetricSpecIndex: number,
  ) => void;
}

export const SchemaTable = React.memo(function SchemaTable(props: SchemaTableProps) {
  const {
    sampleBundle,
    columnFilter,
    selectedDimensionSpecIndex,
    selectedMetricSpecIndex,
    onDimensionOrMetricSelect,
  } = props;
  const { headerAndRows, dimensionsSpec, metricsSpec } = sampleBundle;

  const dimensionMetricSortedHeader = sortWithPrefixSuffix(
    headerAndRows.header,
    ['__time'],
    metricsSpec.map(getMetricSpecName),
    null,
  );

  return (
    <ReactTable
      className="schema-table -striped -highlight"
      data={headerAndRows.rows}
      columns={filterMap(dimensionMetricSortedHeader, (columnName, i) => {
        if (!caseInsensitiveContains(columnName, columnFilter)) return;

        const metricSpecIndex = metricsSpec.findIndex(m => getMetricSpecName(m) === columnName);
        const metricSpec = metricsSpec[metricSpecIndex];

        if (metricSpec) {
          const columnClassName = classNames('metric', {
            selected: metricSpec && metricSpecIndex === selectedMetricSpecIndex,
          });
          return {
            Header: (
              <div
                className="clickable"
                onClick={() =>
                  onDimensionOrMetricSelect(undefined, -1, metricSpec, metricSpecIndex)
                }
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">{metricSpec.type}&nbsp;</div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
            Cell: row => <TableCell value={row.value} />,
          };
        } else {
          const timestamp = columnName === '__time';
          const dimensionSpecIndex = dimensionsSpec.dimensions
            ? dimensionsSpec.dimensions.findIndex(d => getDimensionSpecName(d) === columnName)
            : -1;
          const dimensionSpec = dimensionsSpec.dimensions
            ? dimensionsSpec.dimensions[dimensionSpecIndex]
            : null;
          const dimensionSpecType = dimensionSpec ? getDimensionSpecType(dimensionSpec) : null;

          const columnClassName = classNames(
            timestamp ? 'timestamp' : 'dimension',
            dimensionSpecType || 'string',
            {
              selected: dimensionSpec && dimensionSpecIndex === selectedDimensionSpecIndex,
            },
          );
          return {
            Header: (
              <div
                className="clickable"
                onClick={() => {
                  if (timestamp) {
                    onDimensionOrMetricSelect(undefined, -1, undefined, -1);
                    return;
                  }

                  if (!dimensionSpec) return;
                  onDimensionOrMetricSelect(
                    inflateDimensionSpec(dimensionSpec),
                    dimensionSpecIndex,
                    undefined,
                    -1,
                  );
                }}
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">
                  {timestamp ? 'long (time column)' : dimensionSpecType || 'string (auto)'}&nbsp;
                </div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            accessor: (row: SampleEntry) => (row.parsed ? row.parsed[columnName] : null),
            Cell: row => <TableCell value={timestamp ? new Date(row.value) : row.value} />,
          };
        }
      })}
      defaultPageSize={50}
      showPagination={false}
      sortable={false}
    />
  );
});
