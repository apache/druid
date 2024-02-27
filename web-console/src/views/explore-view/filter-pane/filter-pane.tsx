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

import { Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { FilterPattern, SqlExpression } from '@druid-toolkit/query';
import { filterPatternsToExpression, fitFilterPatterns } from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';
import classNames from 'classnames';
import React, { forwardRef, useImperativeHandle, useState } from 'react';

import { DroppableContainer } from '../droppable-container/droppable-container';
import type { Dataset } from '../utils';

import { FilterMenu } from './filter-menu/filter-menu';
import { formatPatternWithoutNegation, initPatternForColumn } from './pattern-helpers';

import './filter-pane.scss';

export interface FilterPaneProps {
  dataset: Dataset | undefined;
  filter: SqlExpression;
  onFilterChange(filter: SqlExpression): void;
  queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]>;
}

export const FilterPane = forwardRef(function FilterPane(props: FilterPaneProps, ref) {
  const { dataset, filter, onFilterChange, queryDruidSql } = props;
  const patterns = fitFilterPatterns(filter);

  const [menuIndex, setMenuIndex] = useState<number>(-1);
  const [menuNew, setMenuNew] = useState<{ column?: ExpressionMeta }>();

  function filterOn(column: ExpressionMeta) {
    const relevantPatternIndex = patterns.findIndex(
      pattern =>
        pattern.type !== 'custom' && pattern.column === column.expression.getFirstColumnName(),
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
    [patterns],
  );

  function changePatterns(newPatterns: FilterPattern[]) {
    onFilterChange(filterPatternsToExpression(newPatterns));
  }

  return (
    <DroppableContainer className="filter-pane" onDropColumn={filterOn}>
      <Button className="filter-label" minimal text="Filter:" />
      {patterns.map((pattern, i) => {
        return (
          <div className="filter-pill" key={i}>
            {dataset ? (
              <Popover2
                isOpen={i === menuIndex}
                onClose={() => setMenuIndex(-1)}
                content={
                  <FilterMenu
                    dataset={dataset}
                    filter={filter}
                    initPattern={pattern}
                    onPatternChange={newPattern => {
                      changePatterns(patterns.map((c, idx) => (idx === i ? newPattern : c)));
                    }}
                    onClose={() => {
                      setMenuIndex(-1);
                    }}
                    queryDruidSql={queryDruidSql}
                  />
                }
                position="bottom"
              >
                <Button
                  className={classNames('filter-text-button', { negated: pattern.negated })}
                  minimal
                  text={formatPatternWithoutNegation(pattern)}
                  onClick={() => setMenuIndex(i)}
                />
              </Popover2>
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
      {dataset && (
        <Popover2
          className="add-button"
          isOpen={Boolean(menuNew)}
          position="bottom"
          onClose={() => setMenuNew(undefined)}
          content={
            <FilterMenu
              dataset={dataset}
              filter={filter}
              initPattern={menuNew?.column ? initPatternForColumn(menuNew?.column) : undefined}
              onPatternChange={newPattern => {
                changePatterns(patterns.concat(newPattern));
              }}
              onClose={() => {
                setMenuNew(undefined);
              }}
              queryDruidSql={queryDruidSql}
            />
          }
        >
          <Button icon={IconNames.PLUS} onClick={() => setMenuNew({})} minimal />
        </Popover2>
      )}
    </DroppableContainer>
  );
});
