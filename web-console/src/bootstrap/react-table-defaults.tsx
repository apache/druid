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

import React from 'react';
import { Filter, ReactTableDefaults } from 'react-table';

import { Loader } from '../components';
import { booleanCustomTableFilter, countBy, makeTextFilter } from '../utils';

import { ReactTableCustomPagination } from './react-table-custom-pagination';

/* tslint:disable:max-classes-per-file */

class NoData extends React.PureComponent {
  render(): JSX.Element | null {
    const { children } = this.props;
    if (!children) return null;
    return <div className="rt-noData">{children}</div>;
  }
}

/* tslint:enable:max-classes-per-file */

Object.assign(ReactTableDefaults, {
  className: '-striped -highlight',
  defaultFilterMethod: (filter: Filter, row: any) => {
    const id = filter.pivotId || filter.id;
    return booleanCustomTableFilter(filter, row[id]);
  },
  LoadingComponent: Loader,
  loadingText: '',
  NoDataComponent: NoData,
  FilterComponent: makeTextFilter(),
  PaginationComponent: ReactTableCustomPagination,
  AggregatedComponent: (opt: any) => {
    const { subRows, column } = opt;
    const previewValues = subRows
      .filter((d: any) => typeof d[column.id] !== 'undefined')
      .map((row: any) => row[column.id]);
    const previewCount = countBy(previewValues);
    return (
      <span>
        {Object.keys(previewCount)
          .sort()
          .map(v => `${v} (${previewCount[v]})`)
          .join(', ')}
      </span>
    );
  },
  defaultPageSize: 20,
});
