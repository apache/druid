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

import { Button, Menu, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

import { MenuCheckbox } from '../menu-checkbox/menu-checkbox';

import './table-column-selector.scss';

export type TableColumnSelectorColumn = string | { text: string; label: string };

function getColumnName(c: TableColumnSelectorColumn) {
  return typeof c === 'string' ? c : c.text;
}

interface TableColumnSelectorProps {
  columns: TableColumnSelectorColumn[];
  onChange: (columnName: string) => void;
  onClose?: (added: number) => void;
  tableColumnsHidden: string[];
}

export const TableColumnSelector = React.memo(function TableColumnSelector(
  props: TableColumnSelectorProps,
) {
  const { columns, onChange, onClose, tableColumnsHidden } = props;
  const [added, setAdded] = useState(0);

  const isColumnShown = (column: TableColumnSelectorColumn) =>
    !tableColumnsHidden.includes(getColumnName(column));

  const checkboxes = (
    <Menu className="table-column-selector-menu">
      {columns.map(column => {
        const columnName = getColumnName(column);
        return (
          <MenuCheckbox
            text={columnName}
            label={typeof column === 'string' ? undefined : column.label}
            key={columnName}
            checked={isColumnShown(column)}
            onChange={() => {
              if (!isColumnShown(column)) {
                setAdded(added + 1);
              }
              onChange(columnName);
            }}
          />
        );
      })}
    </Menu>
  );

  const counterText = `(${columns.filter(isColumnShown).length}/${columns.length})`;

  return (
    <Popover2
      className="table-column-selector"
      content={checkboxes}
      position={Position.BOTTOM_RIGHT}
      onOpened={() => setAdded(0)}
      onClose={() => {
        if (!onClose) return;
        onClose(added);
      }}
    >
      <Button rightIcon={IconNames.CARET_DOWN}>
        Columns <span className="counter">{counterText}</span>
      </Button>
    </Popover2>
  );
});
