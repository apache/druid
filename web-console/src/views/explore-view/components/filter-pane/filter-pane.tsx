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
import type { CancelToken } from 'axios';
import type { Timezone } from 'chronoshift';
import classNames from 'classnames';
import { isDate } from 'date-fns';
import type {
  Column,
  FilterPattern,
  QueryResult,
  SqlExpression,
  SqlQuery,
} from 'druid-query-toolkit';
import { filterPatternsToExpression, fitFilterPatterns } from 'druid-query-toolkit';
import { forwardRef, useImperativeHandle, useMemo, useState } from 'react';

import { useQueryManager } from '../../../../hooks';
import { prettyFormatIsoDateWithMsIfNeeded, without } from '../../../../utils';
import type { QuerySource } from '../../models';
import {
  formatPatternWithoutNegation,
  initPatternForColumn,
  patternToBoundsQuery,
} from '../../utils';
import { DroppableContainer } from '../droppable-container/droppable-container';

import { FilterMenu } from './filter-menu/filter-menu';

import './filter-pane.scss';

export interface FilterPaneProps {
  querySource: QuerySource | undefined;
  extraFilter: SqlExpression;
  timezone: Timezone;
  filter: SqlExpression;
  onFilterChange(filter: SqlExpression): void;
  runSqlQuery(query: string | SqlQuery, cancelToken?: CancelToken): Promise<QueryResult>;
  onAddToSourceQueryAsColumn?: (expression: SqlExpression) => void;
  onMoveToSourceQueryAsClause?: (expression: SqlExpression, changeWhere?: SqlExpression) => void;
}

export const FilterPane = forwardRef(function FilterPane(props: FilterPaneProps, ref) {
  const {
    querySource,
    extraFilter,
    timezone,
    filter,
    onFilterChange,
    runSqlQuery,
    onAddToSourceQueryAsColumn,
    onMoveToSourceQueryAsClause,
  } = props;
  const patterns = useMemo(() => fitFilterPatterns(filter), [filter]);

  const [menuIndex, setMenuIndex] = useState<number>(-1);
  const [menuNew, setMenuNew] = useState<{ column?: Column }>();

  const boundsQuery: string | undefined = useMemo(() => {
    if (!querySource) return;
    const relativePatterns = patterns.filter(p => p.type === 'timeRelative');
    if (relativePatterns.length !== 1) return;
    return patternToBoundsQuery(querySource.query, relativePatterns[0])?.toString();
  }, [querySource, patterns]);

  const [boundsState] = useQueryManager<string, [Date, Date]>({
    query: boundsQuery,
    processQuery: async (query, cancelToken) => {
      const boundsData = await runSqlQuery(query, cancelToken);
      const startEndRecord = boundsData.toObjectArray()[0];
      if (!startEndRecord || !isDate(startEndRecord.start) || !isDate(startEndRecord.end)) {
        throw new Error('Unexpected result');
      }
      return [startEndRecord.start, startEndRecord.end];
    },
  });

  function filterTooltip(pattern: FilterPattern): string | undefined {
    if (pattern.type !== 'timeRelative') return;
    if (boundsState.isLoading()) return 'Loading...';
    if (boundsState.isError()) return boundsState.getErrorMessage();
    if (!boundsState.data) return;
    const [start, end] = boundsState.data;
    return `${prettyFormatIsoDateWithMsIfNeeded(
      start,
    )} → ${prettyFormatIsoDateWithMsIfNeeded(end)}`;
  }

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
                    extraFilter={extraFilter}
                    filter={filterPatternsToExpression(without(patterns, pattern))}
                    initPattern={pattern}
                    onPatternChange={newPattern => {
                      changePatterns(patterns.map((c, idx) => (idx === i ? newPattern : c)));
                    }}
                    onClose={() => {
                      setMenuIndex(-1);
                    }}
                    runSqlQuery={runSqlQuery}
                    timeBounds={boundsState.data}
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
                  text={formatPatternWithoutNegation(pattern, timezone)}
                  onClick={() => setMenuIndex(i)}
                  data-tooltip={i !== menuIndex ? filterTooltip(pattern) : undefined}
                />
              </Popover>
            ) : (
              <Button
                className={classNames('filter-text-button', { negated: pattern.negated })}
                minimal
                text={formatPatternWithoutNegation(pattern, timezone)}
                disabled
              />
            )}
            <Button
              className="remove"
              icon={IconNames.CROSS}
              minimal
              small
              onClick={() => changePatterns(patterns.filter((_clause, idx) => idx !== i))}
              data-tooltip="Remove filter"
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
              extraFilter={extraFilter}
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
            data-tooltip={patterns.length ? 'Add filter' : undefined}
          />
        </Popover>
      ) : (
        <Button
          icon={IconNames.PLUS}
          text={patterns.length ? undefined : 'Add filter'}
          disabled
          minimal
          data-tooltip="No query source, unable to query"
        />
      )}
    </DroppableContainer>
  );
});
