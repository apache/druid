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

import {
  Button,
  ButtonGroup,
  Callout,
  FormGroup,
  HTMLSelect,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type {
  Column,
  FilterPattern,
  FilterPatternType,
  QueryResult,
  SqlQuery,
} from 'druid-query-toolkit';
import {
  C,
  changeFilterPatternType,
  filterPatternToExpression,
  fitFilterPattern,
  SqlExpression,
} from 'druid-query-toolkit';
import type { JSX } from 'react';
import React, { useState } from 'react';

import { AppToaster } from '../../../../../singletons';
import type { QuerySource } from '../../../models';
import { formatPatternWithoutNegation, initPatternForColumn } from '../../../utils';
import { ColumnPicker } from '../../column-picker/column-picker';
import { ColumnPickerMenu } from '../../column-picker-menu/column-picker-menu';
import { SqlInput } from '../../sql-input/sql-input';

import { ContainsFilterControl } from './contains-filter-control/contains-filter-control';
import { NumberRangeFilterControl } from './number-range-filter-control/number-range-filter-control';
import { RegexpFilterControl } from './regexp-filter-control/regexp-filter-control';
import { TimeIntervalFilterControl } from './time-interval-filter-control/time-interval-filter-control';
import { TimeRelativeFilterControl } from './time-relative-filter-control/time-relative-filter-control';
import { ValuesFilterControl } from './values-filter-control/values-filter-control';

import './filter-menu.scss';

const DEFAULT_PATTERN_TYPES: FilterPatternType[] = [
  'values',
  'contains',
  'regexp',
  'numberRange',
  'timeRelative',
  'timeInterval',
];

function getPattenTypesForColumn(column: Column | undefined): FilterPatternType[] {
  if (!column) return DEFAULT_PATTERN_TYPES;

  switch (column.sqlType) {
    case 'TIMESTAMP':
      return ['timeRelative', 'timeInterval', 'numberRange', 'values', 'contains', 'regexp'];

    case 'BOOLEAN':
      return ['values', 'contains', 'regexp', 'numberRange'];

    case 'BIGINT':
    case 'DOUBLE':
    case 'FLOAT':
      return ['numberRange', 'values'];

    default: //  VARCHAR also gets default
      return DEFAULT_PATTERN_TYPES;
  }
}

const PATTERN_TYPE_TO_NAME: Record<FilterPatternType, string> = {
  values: 'Values',
  contains: 'Contains',
  regexp: 'Regular expression',
  mvContains: 'Multi-value contains',
  numberRange: 'Number range',
  timeRelative: 'Time relative',
  timeInterval: 'Time interval',
  custom: 'Custom',
};

type FilterMenuTab = 'compose' | 'sql';

export interface FilterMenuProps {
  querySource: QuerySource;
  filter: SqlExpression;
  initPattern?: FilterPattern;
  onPatternChange(newPattern: FilterPattern): void;
  onClose(): void;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
  onAddToSourceQueryAsColumn?(expression: SqlExpression): void;
  onMoveToSourceQueryAsClause?(expression: SqlExpression): void;
}

export const FilterMenu = React.memo(function FilterMenu(props: FilterMenuProps) {
  const {
    querySource,
    filter,
    initPattern,
    onPatternChange,
    onClose,
    runSqlQuery,
    onAddToSourceQueryAsColumn,
    onMoveToSourceQueryAsClause,
  } = props;

  const [tab, setTab] = useState<FilterMenuTab>(initPattern?.type === 'custom' ? 'sql' : 'compose');
  const [formula, setFormula] = useState<string>(
    initPattern?.type === 'custom' ? filterPatternToExpression(initPattern).toString() : '',
  );
  const [pattern, setPattern] = useState<FilterPattern | undefined>(initPattern);
  const { columns } = querySource;

  function onAcceptPattern(pattern: FilterPattern) {
    onPatternChange(pattern);
    onClose();
  }

  const negated = Boolean(pattern?.negated);

  let cont: JSX.Element;
  switch (pattern?.type) {
    case 'values':
      cont = (
        <ValuesFilterControl
          querySource={querySource}
          filter={filter.removeColumnFromAnd(pattern.column)}
          filterPattern={pattern}
          setFilterPattern={setPattern}
          runSqlQuery={runSqlQuery}
        />
      );
      break;

    case 'contains':
      cont = (
        <ContainsFilterControl
          querySource={querySource}
          filter={filter.removeColumnFromAnd(pattern.column)}
          filterPattern={pattern}
          setFilterPattern={setPattern}
          runSqlQuery={runSqlQuery}
        />
      );
      break;

    case 'regexp':
      cont = (
        <RegexpFilterControl
          querySource={querySource}
          filter={filter.removeColumnFromAnd(pattern.column)}
          filterPattern={pattern}
          setFilterPattern={setPattern}
          runSqlQuery={runSqlQuery}
        />
      );
      break;

    case 'numberRange':
      cont = (
        <NumberRangeFilterControl
          querySource={querySource}
          filterPattern={pattern}
          setFilterPattern={setPattern}
        />
      );
      break;

    case 'timeInterval':
      cont = (
        <TimeIntervalFilterControl
          querySource={querySource}
          filterPattern={pattern}
          setFilterPattern={setPattern}
        />
      );
      break;

    case 'timeRelative':
      cont = (
        <TimeRelativeFilterControl
          querySource={querySource}
          filterPattern={pattern}
          setFilterPattern={setPattern}
        />
      );
      break;

    case 'mvContains':
    case 'custom': {
      const columnName: string | undefined =
        pattern.type === 'custom'
          ? pattern.expression?.getFirstColumnName()
          : (pattern as any).column;
      const column = columns.find(({ name }) => name === columnName);
      cont = (
        <FormGroup>
          <Callout>
            <p>The current filter can only be edited as SQL.</p>
            <p>
              <a onClick={() => setTab('sql')}>Continue editing in SQL.</a>
            </p>
            {column && (
              <p>
                <a onClick={() => setPattern(initPatternForColumn(column))}>{`Compose on column ${C(
                  column.name,
                )}.`}</a>
              </p>
            )}
          </Callout>
        </FormGroup>
      );
      break;
    }

    default:
      cont = <div>Pattern no set</div>;
      break;
  }

  function parseFormula(): SqlExpression | undefined {
    try {
      return SqlExpression.parse(formula);
    } catch (e) {
      AppToaster.show({
        intent: Intent.DANGER,
        message: e.message,
      });
      return;
    }
  }

  return (
    <div className="filter-menu">
      <FormGroup>
        <ButtonGroup className="tab-bar" fill>
          <Button
            text="Compose"
            active={tab === 'compose'}
            onClick={() => {
              if (formula) {
                const parsedFormula = parseFormula();
                if (!parsedFormula) return;
                setPattern(fitFilterPattern(parsedFormula));
              } else {
                setPattern(undefined);
              }
              setTab('compose');
            }}
          />
          <Button
            text="SQL"
            active={tab === 'sql'}
            onClick={() => {
              setFormula(pattern ? filterPatternToExpression(pattern).toString() : '');
              setTab('sql');
            }}
          />
        </ButtonGroup>
      </FormGroup>
      {tab === 'compose' &&
        (pattern ? (
          <>
            {pattern.type !== 'custom' && (
              <div className="controls">
                <FormGroup className="column-form-group" label="Column">
                  <ColumnPicker
                    availableColumns={querySource.columns}
                    selectedColumnName={pattern.column}
                    onSelectedColumnNameChange={selectedColumnName => {
                      setPattern({ ...pattern, column: selectedColumnName });
                    }}
                    fill
                  />
                </FormGroup>
                <FormGroup label="Filter type">
                  <HTMLSelect
                    className="type-selector"
                    value={pattern.type}
                    onChange={e =>
                      setPattern(
                        changeFilterPatternType(pattern, e.target.value as FilterPatternType),
                      )
                    }
                  >
                    {getPattenTypesForColumn(
                      querySource.columns.find(c => c.name === pattern.column),
                    ).map(type => (
                      <option key={type} value={type}>
                        {PATTERN_TYPE_TO_NAME[type]}
                      </option>
                    ))}
                  </HTMLSelect>
                </FormGroup>
                <FormGroup>
                  <ButtonGroup>
                    <Button
                      icon={IconNames.FILTER}
                      active={!negated}
                      onClick={() => setPattern({ ...pattern, negated: false })}
                      data-tooltip="Include"
                    />
                    <Button
                      icon={IconNames.FILTER_REMOVE}
                      active={negated}
                      onClick={() => setPattern({ ...pattern, negated: true })}
                      data-tooltip="Exclude"
                    />
                  </ButtonGroup>
                </FormGroup>
              </div>
            )}
            {cont}
          </>
        ) : (
          <ColumnPickerMenu
            columns={columns}
            onSelectColumn={c => setPattern(initPatternForColumn(c))}
            rightIconForColumn={c =>
              filter.containsColumnName(c.name) ? IconNames.FILTER : undefined
            }
            shouldDismissPopover={false}
          />
        ))}
      {tab === 'sql' && (
        <FormGroup>
          <SqlInput
            value={formula}
            onValueChange={sql => setFormula(sql)}
            columns={columns}
            placeholder="SQL expression"
            editorHeight={250}
            autoFocus
          />
        </FormGroup>
      )}
      {(pattern || tab === 'sql') && (
        <div className="button-bar">
          {pattern && onAddToSourceQueryAsColumn && onMoveToSourceQueryAsClause && (
            <Popover
              position={Position.BOTTOM_LEFT}
              content={
                <Menu>
                  <MenuItem
                    text="Add as boolean column to the source query"
                    onClick={() => {
                      if (tab === 'compose') {
                        onAddToSourceQueryAsColumn(
                          filterPatternToExpression(pattern).as(
                            formatPatternWithoutNegation(pattern),
                          ),
                        );
                      } else {
                        const parsedFormula = parseFormula();
                        if (!parsedFormula) return;
                        onAddToSourceQueryAsColumn(parsedFormula.as(formula));
                      }
                      onClose();
                    }}
                  />
                  <MenuItem
                    text="Move where clause to the source query"
                    onClick={() => {
                      if (tab === 'compose') {
                        onMoveToSourceQueryAsClause(filterPatternToExpression(pattern));
                      } else {
                        const parsedFormula = parseFormula();
                        if (!parsedFormula) return;
                        onMoveToSourceQueryAsClause(parsedFormula);
                      }
                      onClose();
                    }}
                  />
                </Menu>
              }
            >
              <Button icon={IconNames.TH_DERIVED} minimal />
            </Popover>
          )}
          <div className="button-separator" />
          <Button text="Cancel" onClick={onClose} />
          <Button
            intent={Intent.PRIMARY}
            text="Apply"
            disabled={tab === 'sql' && formula === ''}
            onClick={() => {
              if (tab === 'compose') {
                if (pattern) {
                  onAcceptPattern(pattern);
                }
              } else {
                const parsedFormula = parseFormula();
                if (!parsedFormula) return;
                onAcceptPattern({ type: 'custom', negated: false, expression: parsedFormula });
              }
            }}
          />
        </div>
      )}
    </div>
  );
});
