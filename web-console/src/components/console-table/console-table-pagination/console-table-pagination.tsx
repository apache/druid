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

import { Button, ButtonGroup, Menu, MenuItem, PopoverNext } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { formatInteger, tickIcon } from '../../../utils';

import { PageJumpDialog } from './page-jump-dialog/page-jump-dialog';

export interface ConsoleTablePaginationProps {
  page: number;
  pages: number;
  pageSize: number;
  pageSizeOptions: number[];
  showPageJump: boolean;
  canPrevious: boolean;
  canNext: boolean;
  onPageChange(page: number): void;
  onPageSizeChange(pageSize: number): void;
  ofText: string;
  rowCount: number;
}

export const ConsoleTablePagination = React.memo(function ConsoleTablePagination(
  props: ConsoleTablePaginationProps,
) {
  const {
    page,
    pages,
    onPageChange,
    pageSize,
    onPageSizeChange,
    pageSizeOptions,
    showPageJump,
    canPrevious,
    canNext,
    ofText,
    rowCount,
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
  if (rowCount && (page === 0 || rowCount > pageSize)) {
    end = Math.min(end, rowCount);
  }

  let pageInfo = 'Showing';
  if (end) {
    pageInfo += ` ${formatInteger(start)}-${formatInteger(end)}`;
  } else {
    pageInfo += '...';
  }
  if (ofText === 'of' && rowCount) {
    pageInfo += ` ${ofText} ${formatInteger(rowCount)}`;
  } else if (ofText) {
    pageInfo += ` ${ofText}`;
  }

  const pageJumpMenuItem = renderPageJumpMenuItem();
  const pageSizeChangeMenuItem = renderPageSizeChangeMenuItem();

  return (
    <div className="console-table-pagination">
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
        <PopoverNext
          placement="top-start"
          disabled={!pageJumpMenuItem && !pageSizeChangeMenuItem}
          content={
            <Menu>
              {pageJumpMenuItem}
              {pageSizeChangeMenuItem}
            </Menu>
          }
          lazy
          shouldReturnFocusOnClose={false}
        >
          <Button minimal text={pageInfo} />
        </PopoverNext>
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
