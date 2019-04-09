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

import {Button, Classes, HTMLSelect, NumericInput} from '@blueprintjs/core';
import * as React from 'react';
import { Filter, ReactTableDefaults } from 'react-table';

import { Loader } from '../components/loader';
import { countBy, makeTextFilter } from '../utils';

import './react-table-defaults.scss';

/* tslint:disable:max-classes-per-file */

class FullButton extends React.Component {
  render() {
    return <Button fill {...this.props}/>;
  }
}

class NoData extends React.Component {
  render() {
    const { children } = this.props;
    if (!children) return null;
    return <div className="rt-noData">{children}</div>;
  }
}

const renderPageJump = ({ onChange, value, onBlur, onKeyPress, inputType, pageJumpText }: {[key: string]: any}) => {
  return <div className="-pageJump">
    <input
      className={Classes.INPUT}
      aria-label={pageJumpText}
      type={inputType}
      onChange={onChange}
      value={value}
      onBlur={onBlur}
      onKeyPress={onKeyPress}
    />
  </div>;
};

const renderPageSizeOptions = ({ pageSize, pageSizeOptions, rowsSelectorText, onPageSizeChange, rowsText}: {[key: string]: any}) => {
  return (
    <div className={`select-wrap -pageSizeOptions`}>
      <HTMLSelect
        aria-label={rowsSelectorText}
        onChange={e => onPageSizeChange(Number(e.target.value))}
        value={pageSize}
      >
        {pageSizeOptions.map((option: any, i: number) => (
          <option key={i} value={option}>
            {`${option} ${rowsText}`}
          </option>
        ))}
      </HTMLSelect>
    </div>
  );
};

/* tslint:enable:max-classes-per-file */

Object.assign(ReactTableDefaults, {
  defaultFilterMethod: (filter: Filter, row: any, column: any) => {
    const id = filter.pivotId || filter.id;
    return row[id] !== undefined ? String(row[id]).includes(filter.value) : true;
  },
  LoadingComponent: Loader,
  loadingText: '',
  NoDataComponent: NoData,
  FilterComponent: makeTextFilter(),
  PreviousComponent: FullButton,
  NextComponent: FullButton,
  renderPageJump: renderPageJump,
  renderPageSizeOptions: renderPageSizeOptions,
  AggregatedComponent: (opt: any) => {
    const { subRows, column } = opt;
    const previewValues = subRows.filter((d: any) => typeof d[column.id] !== 'undefined').map((row: any) => row[column.id]);
    const previewCount = countBy(previewValues);
    return <span>{Object.keys(previewCount).sort().map(v => `${v} (${previewCount[v]})`).join(', ')}</span>;
  }
});
