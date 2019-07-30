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

import { Button, Classes, HTMLSelect } from '@blueprintjs/core';
import React from 'react';

import './react-table-custom-pagination.scss';

interface ReactTableCustomPaginationProps {
  pages: number;
  page: number;
  showPageSizeOptions: boolean;
  pageSizeOptions: number[];
  pageSize: number;
  showPageJump: boolean;
  canPrevious: boolean;
  canNext: boolean;
  onPageSizeChange: any;
  previousText: string;
  nextText: string;
  onPageChange: any;
  ofText: string;
  pageText: string;
  rowsText: string;
  style: Record<string, any>;
}

interface ReactTableCustomPaginationState {
  page: string | number;
}

export class ReactTableCustomPagination extends React.PureComponent<
  ReactTableCustomPaginationProps,
  ReactTableCustomPaginationState
> {
  constructor(props: ReactTableCustomPaginationProps) {
    super(props);

    this.state = {
      page: props.page,
    };
  }

  componentWillReceiveProps(nextProps: ReactTableCustomPaginationProps) {
    this.setState({ page: nextProps.page });
  }

  getSafePage = (page: any) => {
    if (Number.isNaN(page)) {
      page = this.props.page;
    }
    return Math.min(Math.max(page, 0), this.props.pages - 1);
  };

  changePage = (page: any) => {
    page = this.getSafePage(page);
    this.setState({ page });
    if (this.props.page !== page) {
      this.props.onPageChange(page);
    }
  };

  applyPage = (e: any) => {
    if (e) {
      e.preventDefault();
    }
    const page = this.state.page;
    this.changePage(page === '' ? this.props.page : page);
  };

  render(): JSX.Element {
    const {
      pages,
      page,
      showPageSizeOptions,
      pageSizeOptions,
      pageSize,
      showPageJump,
      canPrevious,
      canNext,
      onPageSizeChange,
      style,
      previousText,
      nextText,
      ofText,
      pageText,
      rowsText,
    } = this.props;

    return (
      <div className="-pagination" style={style}>
        <div className="-previous">
          <Button
            fill
            onClick={() => {
              if (!canPrevious) return;
              this.changePage(page - 1);
            }}
            disabled={!canPrevious}
          >
            {previousText}
          </Button>
        </div>
        <div className="-center">
          <span className="-pageInfo">
            {pageText}{' '}
            {showPageJump ? (
              <div className="-pageJump">
                <input
                  className={Classes.INPUT}
                  type={this.state.page === '' ? 'text' : 'number'}
                  onChange={e => {
                    const val: string = e.target.value;
                    const page: number = parseInt(val, 10) - 1;
                    if (val === '') {
                      return this.setState({ page: val });
                    }
                    this.setState({ page: this.getSafePage(page) });
                  }}
                  value={this.state.page === '' ? '' : (this.state.page as number) + 1}
                  onBlur={this.applyPage}
                  onKeyPress={e => {
                    if (e.which === 13 || e.keyCode === 13) {
                      this.applyPage(e);
                    }
                  }}
                />
              </div>
            ) : (
              <span className="-currentPage">{page + 1}</span>
            )}{' '}
            {ofText} <span className="-totalPages">{pages || 1}</span>
          </span>
          {showPageSizeOptions && (
            <span className="select-wrap -pageSizeOptions">
              <HTMLSelect onChange={e => onPageSizeChange(Number(e.target.value))} value={pageSize}>
                {pageSizeOptions.map((option: any, i: number) => (
                  <option key={i} value={option}>
                    {`${option} ${rowsText}`}
                  </option>
                ))}
              </HTMLSelect>
            </span>
          )}
        </div>
        <div className="-next">
          <Button
            fill
            onClick={() => {
              if (!canNext) return;
              this.changePage(page + 1);
            }}
            disabled={!canNext}
          >
            {nextText}
          </Button>
        </div>
      </div>
    );
  }
}
