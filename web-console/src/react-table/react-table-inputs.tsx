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

import { Button, HTMLSelect, Icon, InputGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React, { useState } from 'react';
import { Column, ReactTableFunction } from 'react-table';

import {
  combineModeAndNeedle,
  FILTER_MODES,
  FILTER_MODES_NO_COMPARISON,
  filterModeToIcon,
  filterModeToTitle,
  parseFilterModeAndNeedle,
} from './react-table-utils';

interface FilterRendererProps {
  column: Column;
  filter: any;
  onChange: ReactTableFunction;
  key?: string;
}

export function GenericFilterInput({ column, filter, onChange, key }: FilterRendererProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [focused, setFocused] = useState(false);

  const enableComparisons = String(column.headerClassName).includes('enable-comparisons');

  const { mode, needle } = (filter ? parseFilterModeAndNeedle(filter, true) : undefined) || {
    mode: '~',
    needle: '',
  };

  return (
    <InputGroup
      className={classNames('generic-filter-input', {
        'hide-icon': !filter && !(menuOpen || focused),
      })}
      key={key}
      leftElement={
        <Popover2
          placement="bottom-start"
          minimal
          isOpen={menuOpen}
          onInteraction={setMenuOpen}
          content={
            <Menu>
              {(enableComparisons ? FILTER_MODES : FILTER_MODES_NO_COMPARISON).map((m, i) => (
                <MenuItem
                  key={i}
                  icon={filterModeToIcon(m)}
                  text={filterModeToTitle(m)}
                  onClick={() => onChange(combineModeAndNeedle(m, needle))}
                  labelElement={m === mode ? <Icon icon={IconNames.TICK} /> : undefined}
                />
              ))}
            </Menu>
          }
        >
          <Button className="filter-mode-button" icon={filterModeToIcon(mode)} minimal />
        </Popover2>
      }
      value={needle}
      onChange={e => onChange(combineModeAndNeedle(mode, e.target.value))}
      rightElement={
        filter ? <Button icon={IconNames.CROSS} minimal onClick={() => onChange('')} /> : undefined
      }
      onFocus={() => setFocused(true)}
      onBlur={e => {
        setFocused(false);
        if (filter && !e.target.value) onChange('');
      }}
    />
  );
}

export function BooleanFilterInput({ filter, onChange, key }: FilterRendererProps) {
  const filterValue = filter ? filter.value : '';
  return (
    <HTMLSelect
      className="boolean-filter-input"
      key={key}
      style={{ width: '100%' }}
      onChange={(event: any) => onChange(event.target.value)}
      value={filterValue || 'all'}
      fill
    >
      <option value="all">Show all</option>
      <option value="true">true</option>
      <option value="false">false</option>
    </HTMLSelect>
  );
}
