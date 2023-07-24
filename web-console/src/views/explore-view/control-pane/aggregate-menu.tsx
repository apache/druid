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

import { InputGroup, Menu, MenuItem } from '@blueprintjs/core';
import { SqlFunction } from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';
import React, { useState } from 'react';

import { caseInsensitiveContains } from '../../../utils';

import { getPossibleAggregateForColumn } from './helpers';

import './aggregate-menu.scss';

const COUNT_AGG: ExpressionMeta = {
  name: 'Count',
  expression: SqlFunction.count(),
  sqlType: 'BIGINT',
};

export interface AggregateMenuProps {
  columns: ExpressionMeta[];
  onSelectAggregate(aggregate: ExpressionMeta): void;
  onSelectNone?: () => void;
  shouldDismissPopover?: boolean;
}

export const AggregateMenu = function AggregateMenu(props: AggregateMenuProps) {
  const { columns, onSelectAggregate, onSelectNone, shouldDismissPopover } = props;
  const [columnSearch, setColumnSearch] = useState('');

  return (
    <div className="aggregate-menu">
      <InputGroup
        className="search-input"
        value={columnSearch}
        onChange={e => setColumnSearch(e.target.value)}
        placeholder="Search..."
        autoFocus
      />
      <Menu className="inner-menu">
        {onSelectNone && (
          <MenuItem
            text="None"
            onClick={onSelectNone}
            shouldDismissPopover={shouldDismissPopover}
          />
        )}
        <MenuItem text={COUNT_AGG.name} onClick={() => onSelectAggregate(COUNT_AGG)} />
        {columns.map((c, i) => {
          if (!caseInsensitiveContains(c.name, columnSearch)) return;
          const possibleAggregateForColumn = getPossibleAggregateForColumn(c);
          if (!possibleAggregateForColumn.length) return;
          if (possibleAggregateForColumn.length === 1) {
            const a = possibleAggregateForColumn[0];
            return <MenuItem key={i} text={a.name} onClick={() => onSelectAggregate(a)} />;
          } else {
            return (
              <MenuItem key={i} text={`${c.name}...`}>
                {possibleAggregateForColumn.map((a, j) => (
                  <MenuItem key={j} text={a.name} onClick={() => onSelectAggregate(a)} />
                ))}
              </MenuItem>
            );
          }
        })}
      </Menu>
    </div>
  );
};
