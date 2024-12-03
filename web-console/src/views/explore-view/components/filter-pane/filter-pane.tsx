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

import { Button, Popover } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import type {
  Column,
  FilterPattern,
  QueryResult,
  SqlExpression,
  SqlQuery,
} from 'druid-query-toolkit';
import { filterPatternsToExpression, fitFilterPatterns } from 'druid-query-toolkit';
import { forwardRef, useImperativeHandle, useState } from 'react';

import type { QuerySource } from '../../models';
import { formatPatternWithoutNegation, initPatternForColumn } from '../../utils';
import { DroppableContainer } from '../droppable-container/droppable-container';

import { FilterMenu } from './filter-menu/filter-menu';

import './filter-pane.scss';

export interface FilterPaneProps {
  querySource: QuerySource | undefined;
  filter: SqlExpression;
  onFilterChange(filter: SqlExpression): void;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
  onAddToSourceQueryAsColumn?: (expression: SqlExpression) => void;
  onMoveToSourceQueryAsClause?: (expression: SqlExpression, changeWhere?: SqlExpression) => void;
}

export const FilterPane = forwardRef(function FilterPane(props: FilterPaneProps, ref) {
  const {
    querySource,
    filter,
    onFilterChange,
    runSqlQuery,
    onAddToSourceQueryAsColumn,
    onMoveToSourceQueryAsClause,
  } = props;
  const patterns = fitFilterPatterns(filter);

  const [menuIndex, setMenuIndex] = useState<number>(-1);
  const [menuNew, setMenuNew] = useState<{ column?: Column }>();

  function filterOn(column: Column) {
    const relevantPatternIndex = patterns.findIndex(
      pattern => pattern.type !== 'custom' && pattern.column === column.name,
    );
    if (relevantPatternIndex < 0) {
      setMenuNew({ column });
    } else {
      setMenuIndex(relevantPatternIndex);
    }
  }

  useImperativeHandle(
    ref,
    () => ({
      filterOn,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [patterns],
  );

  function changePatterns(newPatterns: FilterPattern[]) {
    onFilterChange(filterPatternsToExpression(newPatterns));
  }

  return (
    <DroppableContainer className="filter-pane" onDropColumn={filterOn}>
      <Button className="filter-icon-button" icon={IconNames.FILTER} minimal disabled />
      {patterns.map((pattern, i) => {
        return (
          <div className="filter-pill" key={i}>
            {querySource ? (
              <Popover
                isOpen={i === menuIndex}
                onClose={() => setMenuIndex(-1)}
                position="bottom"
                content={
                  <FilterMenu
                    querySource={querySource}
                    filter={filter}
                    initPattern={pattern}
                    onPatternChange={newPattern => {
                      changePatterns(patterns.map((c, idx) => (idx === i ? newPattern : c)));
                    }}
                    onClose={() => {
                      setMenuIndex(-1);
                    }}
                    runSqlQuery={runSqlQuery}
                    onAddToSourceQueryAsColumn={onAddToSourceQueryAsColumn}
                    onMoveToSourceQueryAsClause={
                      onMoveToSourceQueryAsClause
                        ? ex => {
                            onMoveToSourceQueryAsClause(
                              ex,
                              filterPatternsToExpression(
                                patterns.filter((_clause, idx) => idx !== i),
                              ),
                            );
                          }
                        : undefined
                    }
                  />
                }
              >
                <Button
                  className={classNames('filter-text-button', { negated: pattern.negated })}
                  minimal
                  text={formatPatternWithoutNegation(pattern)}
                  onClick={() => setMenuIndex(i)}
                />
              </Popover>
            ) : (
              <Button
                className={classNames('filter-text-button', { negated: pattern.negated })}
                minimal
                text={formatPatternWithoutNegation(pattern)}
                disabled
              />
            )}
            <Button
              className="remove"
              icon={IconNames.CROSS}
              minimal
              small
              onClick={() => changePatterns(patterns.filter((_clause, idx) => idx !== i))}
            />
          </div>
        );
      })}
      {querySource ? (
        <Popover
          className="add-button"
          isOpen={Boolean(menuNew)}
          position="bottom"
          onClose={() => setMenuNew(undefined)}
          content={
            <FilterMenu
              querySource={querySource}
              filter={filter}
              initPattern={menuNew?.column ? initPatternForColumn(menuNew?.column) : undefined}
              onPatternChange={newPattern => {
                changePatterns(patterns.concat(newPattern));
              }}
              onClose={() => {
                setMenuNew(undefined);
              }}
              runSqlQuery={runSqlQuery}
              onAddToSourceQueryAsColumn={onAddToSourceQueryAsColumn}
              onMoveToSourceQueryAsClause={onMoveToSourceQueryAsClause}
            />
          }
        >
          <Button
            icon={IconNames.PLUS}
            text={patterns.length ? undefined : 'Add filter'}
            onClick={() => setMenuNew({})}
            minimal
          />
        </Popover>
      ) : (
        <Button
          icon={IconNames.PLUS}
          text={patterns.length ? undefined : 'Add filter'}
          disabled
          minimal
        />
      )}
    </DroppableContainer>
  );
});
