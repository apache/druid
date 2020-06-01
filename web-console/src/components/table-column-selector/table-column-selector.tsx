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

import { Button, Menu, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { MenuCheckbox } from '../menu-checkbox/menu-checkbox';

import './table-column-selector.scss';

interface TableColumnSelectorProps {
  columns: string[];
  onChange: (column: string) => void;
  tableColumnsHidden: string[];
}

export const TableColumnSelector = React.memo(function TableColumnSelector(
  props: TableColumnSelectorProps,
) {
  const { columns, onChange, tableColumnsHidden } = props;

  const isColumnShown = (column: string) => !tableColumnsHidden.includes(column);

  const checkboxes = (
    <Menu className="table-column-selector-menu">
      {columns.map(column => (
        <MenuCheckbox
          label={column}
          key={column}
          checked={isColumnShown(column)}
          onChange={() => onChange(column)}
        />
      ))}
    </Menu>
  );

  const counterText = `(${columns.filter(isColumnShown).length}/${columns.length})`;

  return (
    <Popover
      className="table-column-selector"
      content={checkboxes}
      position={Position.BOTTOM_RIGHT}
    >
      <Button rightIcon={IconNames.CARET_DOWN}>
        Columns <span className="counter">{counterText}</span>
      </Button>
    </Popover>
  );
});
