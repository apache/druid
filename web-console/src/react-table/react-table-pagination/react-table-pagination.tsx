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

import { Button, ButtonGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

import { formatInteger, nonEmptyArray, tickIcon } from '../../utils';

import { PageJumpDialog } from './page-jump-dialog/page-jump-dialog';

import './react-table-pagination.scss';

interface ReactTablePaginationProps {
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
  sortedData?: any[];
  style: Record<string, any>;
}

export const ReactTablePagination = React.memo(function ReactTablePagination(
  props: ReactTablePaginationProps,
) {
  const {
    page,
    pages,
    onPageChange,
    pageSize,
    onPageSizeChange,
    pageSizeOptions,
    showPageJump,
    showPageSizeOptions,
    canPrevious,
    canNext,
    style,
    ofText,
    sortedData,
  } = props;
  const [showPageJumpDialog, setShowPageJumpDialog] = useState(false);

  function changePage(newPage: number) {
    newPage = Math.min(Math.max(newPage, 0), pages - 1);
    if (page !== newPage) {
      onPageChange(newPage);
    }
  }

  function renderPageJumpMenuItem() {
    if (!showPageJump) return;
    return <MenuItem text="Jump to page..." onClick={() => setShowPageJumpDialog(true)} />;
  }

  function renderPageSizeChangeMenuItem() {
    if (!showPageSizeOptions) return;
    return (
      <MenuItem text={`Page size: ${pageSize}`}>
        {pageSizeOptions.map((option, i) => (
          <MenuItem
            key={i}
            icon={tickIcon(option === pageSize)}
            text={String(option)}
            onClick={() => {
              if (option === pageSize) return;
              onPageSizeChange(option);
            }}
          />
        ))}
      </MenuItem>
    );
  }

  const start = page * pageSize + 1;
  let end = page * pageSize + pageSize;
  if (nonEmptyArray(sortedData)) {
    end = Math.min(end, page * pageSize + sortedData.length);
  }

  let pageInfo = 'Showing';
  if (end) {
    pageInfo += ` ${formatInteger(start)}-${formatInteger(end)}`;
  } else {
    pageInfo += '...';
  }
  if (ofText && Array.isArray(sortedData)) {
    pageInfo += ` ${ofText} ${formatInteger(sortedData.length)}`;
  }

  const pageJumpMenuItem = renderPageJumpMenuItem();
  const pageSizeChangeMenuItem = renderPageSizeChangeMenuItem();

  return (
    <div className="react-table-pagination" style={style}>
      <ButtonGroup>
        <Button
          icon={IconNames.CHEVRON_LEFT}
          minimal
          disabled={!canPrevious}
          onClick={() => changePage(page - 1)}
        />
        <Button
          icon={IconNames.CHEVRON_RIGHT}
          minimal
          disabled={!canNext}
          onClick={() => changePage(page + 1)}
        />
        <Popover2
          position="top-left"
          disabled={!pageJumpMenuItem && !pageSizeChangeMenuItem}
          content={
            <Menu>
              {pageJumpMenuItem}
              {pageSizeChangeMenuItem}
            </Menu>
          }
        >
          <Button minimal text={pageInfo} />
        </Popover2>
      </ButtonGroup>
      {showPageJumpDialog && (
        <PageJumpDialog
          initPage={page}
          maxPage={pages}
          onJump={changePage}
          onClose={() => setShowPageJumpDialog(false)}
        />
      )}
    </div>
  );
});
