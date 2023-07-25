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

import { Button, ButtonGroup, FormGroup, HTMLSelect } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { FilterPattern, FilterPatternType, SqlExpression } from '@druid-toolkit/query';
import { changeFilterPatternType, FILTER_PATTERN_TYPES } from '@druid-toolkit/query';
import type { JSX } from 'react';
import React, { useState } from 'react';

import { ColumnPickerMenu } from '../../column-picker-menu/column-picker-menu';
import type { Dataset } from '../../utils';
import { initPatternForColumn } from '../pattern-helpers';

import { ContainsFilterControl } from './contains-filter-control/contains-filter-control';
import { CustomFilterControl } from './custom-filter-control/custom-filter-control';
import { RegexpFilterControl } from './regexp-filter-control/regexp-filter-control';
import { TimeIntervalFilterControl } from './time-interval-filter-control/time-interval-filter-control';
import { TimeRelativeFilterControl } from './time-relative-filter-control/time-relative-filter-control';
import { ValuesFilterControl } from './values-filter-control/values-filter-control';

import './filter-menu.scss';

const PATTERN_TYPE_TO_NAME: Record<FilterPatternType, string> = {
  values: 'Values',
  contains: 'Contains',
  custom: 'Custom',
  mvContains: 'Multi-value contains',
  numberRange: 'Number range',
  regexp: 'Regular expression',
  timeInterval: 'Time interval',
  timeRelative: 'Time relative',
};

export interface FilterMenuProps {
  dataset: Dataset;
  filter: SqlExpression;
  initPattern?: FilterPattern;
  onPatternChange(newPattern: FilterPattern): void;
  onClose(): void;
  queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]>;
}

export const FilterMenu = React.memo(function FilterMenu(props: FilterMenuProps) {
  const { dataset, filter, initPattern, onPatternChange, onClose, queryDruidSql } = props;

  const [pattern, setPattern] = useState<FilterPattern | undefined>(initPattern);
  const [negated, setNegated] = useState(Boolean(pattern?.negated));

  const { columns } = dataset;

  if (!pattern) {
    return (
      <ColumnPickerMenu
        className="filter-menu"
        columns={columns}
        onSelectColumn={c => setPattern(initPatternForColumn(c))}
        iconForColumn={c => (filter.containsColumnName(c.name) ? IconNames.FILTER : undefined)}
        shouldDismissPopover={false}
      />
    );
  }

  function onAcceptPattern(pattern: FilterPattern) {
    onPatternChange({ ...pattern, negated });
    onClose();
  }

  let cont: JSX.Element;
  switch (pattern.type) {
    case 'values':
      cont = (
        <ValuesFilterControl
          dataset={dataset}
          filter={filter.removeColumnFromAnd(pattern.column)}
          initFilterPattern={pattern}
          negated={negated}
          setFilterPattern={onAcceptPattern}
          onClose={onClose}
          queryDruidSql={queryDruidSql}
        />
      );
      break;

    case 'contains':
      cont = (
        <ContainsFilterControl
          dataset={dataset}
          filter={filter.removeColumnFromAnd(pattern.column)}
          initFilterPattern={pattern}
          negated={negated}
          setFilterPattern={onAcceptPattern}
          queryDruidSql={queryDruidSql}
        />
      );
      break;

    case 'regexp':
      cont = (
        <RegexpFilterControl
          dataset={dataset}
          filter={filter.removeColumnFromAnd(pattern.column)}
          initFilterPattern={pattern}
          negated={negated}
          setFilterPattern={onAcceptPattern}
          queryDruidSql={queryDruidSql}
        />
      );
      break;

    case 'timeInterval':
      cont = (
        <TimeIntervalFilterControl
          dataset={dataset}
          initFilterPattern={pattern}
          negated={negated}
          setFilterPattern={onAcceptPattern}
        />
      );
      break;

    case 'timeRelative':
      cont = (
        <TimeRelativeFilterControl
          dataset={dataset}
          initFilterPattern={pattern}
          negated={negated}
          setFilterPattern={onAcceptPattern}
        />
      );
      break;

    case 'custom':
      cont = (
        <CustomFilterControl
          initFilterPattern={pattern}
          negated={negated}
          setFilterPattern={onAcceptPattern}
        />
      );
      break;

    default:
      cont = <div />; // TODO fix
      break;
  }

  return (
    <div className="filter-menu main">
      <FormGroup className="controls">
        <HTMLSelect
          className="type-selector"
          value={pattern.type}
          onChange={e =>
            setPattern(changeFilterPatternType(pattern, e.target.value as FilterPatternType))
          }
        >
          {FILTER_PATTERN_TYPES.map(type => (
            <option key={type} value={type}>
              {PATTERN_TYPE_TO_NAME[type]}
            </option>
          ))}
        </HTMLSelect>
        <ButtonGroup>
          <Button icon={IconNames.FILTER} active={!negated} onClick={() => setNegated(false)} />
          <Button
            icon={IconNames.FILTER_REMOVE}
            active={negated}
            onClick={() => setNegated(true)}
          />
        </ButtonGroup>
      </FormGroup>
      {cont}
    </div>
  );
});
