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

import { Button, Menu, MenuItem, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Column } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { columnToIcon } from '../../../../utils';

export interface ColumnPickerProps {
  availableColumns: readonly Column[] | undefined;
  selectedColumnName: string;
  onSelectedColumnNameChange(selectedColumnName: string): void;
  fill?: boolean;
  disabled?: boolean;
  shouldDismissPopover?: boolean;
}

export const ColumnPicker = React.memo(function ColumnPicker(props: ColumnPickerProps) {
  const {
    availableColumns,
    selectedColumnName,
    onSelectedColumnNameChange,
    fill,
    disabled,
    shouldDismissPopover,
  } = props;
  const [isOpen, setIsOpen] = useState(false);

  const selectedColumn = availableColumns?.find(c => c.name === selectedColumnName);
  return (
    <Popover
      className="column-picker"
      position={Position.BOTTOM_LEFT}
      isOpen={isOpen}
      minimal
      fill={fill}
      disabled={disabled}
      content={
        <Menu style={{ maxHeight: '600px', overflowY: 'scroll' }}>
          {availableColumns?.map((column, i) => (
            <MenuItem
              key={i}
              icon={columnToIcon(column)}
              text={column.name}
              onClick={() => {
                setIsOpen(false);
                onSelectedColumnNameChange(column.name);
              }}
              shouldDismissPopover={shouldDismissPopover}
            />
          )) || <MenuItem text="Loading..." />}
        </Menu>
      }
    >
      <Button
        icon={selectedColumn ? columnToIcon(selectedColumn) : undefined}
        text={selectedColumnName}
        rightIcon={IconNames.CARET_DOWN}
        fill={fill}
        disabled={disabled}
        onClick={() => {
          setIsOpen(true);
        }}
      />
    </Popover>
  );
});
