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
  Classes,
  Icon,
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import type { Column, QueryResult, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import { useState } from 'react';

import { ClearableInput } from '../../../../components';
import { caseInsensitiveContains, columnToIcon, filterMap } from '../../../../utils';
import { DragHelper } from '../../drag-helper';
import type { Measure, QuerySource } from '../../models';
import type { Rename } from '../../utils';

import { ColumnDialog } from './column-dialog/column-dialog';
import { MeasureDialog } from './measure-dialog/measure-dialog';
import { NestedColumnDialog } from './nested-column-dialog/nested-column-dialog';

import './resource-pane.scss';

function makeNiceTitle(name: string): string {
  return name
    .replace(/^[ _-]+|[ _-]+$/g, '')
    .replace(/(^|[_-]+)\w/g, s => {
      // 'hello_world-love' -> 'Hello World Love'
      return s.replace(/[_-]+/, ' ').toUpperCase();
    })
    .replace(/[a-z0-9][A-Z]/g, s => {
      // 'HelloWorld' -> 'Hello World'
      return s[0] + ' ' + s[1];
    });
}

interface ColumnEditorOpenOn {
  expression?: SqlExpression;
  columnToDuplicate?: string;
}

interface MeasureEditorOpenOn {
  measure?: Measure;
  measureToDuplicate?: string;
}

export interface ResourcePaneProps {
  querySource: QuerySource;
  onQueryChange: (newQuery: SqlQuery, rename: Rename | undefined) => void;
  onFilter?: (column: Column) => void;
  onShowColumn(column: Column): void;
  onShowMeasure(measure: Measure): void;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
}

export const ResourcePane = function ResourcePane(props: ResourcePaneProps) {
  const { querySource, onQueryChange, onFilter, onShowColumn, onShowMeasure, runSqlQuery } = props;
  const [columnSearch, setColumnSearch] = useState('');

  const [columnEditorOpenOn, setColumnEditorOpenOn] = useState<ColumnEditorOpenOn | undefined>();
  const [nestedColumnEditorOpenOn, setNestedColumnEditorOpenOn] = useState<
    SqlExpression | undefined
  >();
  const [measureEditorOpenOn, setMeasureEditorOpenOn] = useState<MeasureEditorOpenOn | undefined>();

  function applyUtil(nameTransform: (columnName: string) => string) {
    if (!querySource) return;
    const columnNameMap = querySource.getColumnNameMap(nameTransform);
    onQueryChange(querySource.applyColumnNameMap(columnNameMap), columnNameMap);
  }

  return (
    <div className="resource-pane">
      <ClearableInput
        className="search-input"
        value={columnSearch}
        onChange={setColumnSearch}
        placeholder="Search"
      />
      <div className="list-header column-list-header">
        Columns
        <ButtonGroup className="header-buttons" minimal>
          <Button
            icon={IconNames.PLUS}
            title="Add column"
            onClick={() => setColumnEditorOpenOn({})}
          />
          <Popover
            content={
              <Menu>
                <MenuItem
                  text="Make nice columns titles"
                  onClick={() => applyUtil(makeNiceTitle)}
                />
                <MenuItem
                  text="Uppercase column names"
                  onClick={() => applyUtil(x => x.toUpperCase())}
                />
                <MenuItem
                  text="Lowercase column names"
                  onClick={() => applyUtil(x => x.toLowerCase())}
                />
              </Menu>
            }
          >
            <Button icon={IconNames.MORE} />
          </Popover>
        </ButtonGroup>
      </div>
      <div className="column-resource-list">
        {filterMap(querySource.columns, (column, i) => {
          const columnName = column.name;
          const isNestedColumn = column.nativeType === 'COMPLEX<json>';
          if (!caseInsensitiveContains(columnName, columnSearch)) return;
          return (
            <Popover
              className="column-resource"
              key={i}
              position="right"
              content={
                <Menu>
                  {isNestedColumn ? (
                    <MenuItem
                      icon={IconNames.EXPAND_ALL}
                      text="Expand nested column"
                      onClick={() =>
                        setNestedColumnEditorOpenOn(
                          querySource.getSourceExpressionForColumn(columnName),
                        )
                      }
                    />
                  ) : (
                    <>
                      {onFilter && (
                        <MenuItem
                          icon={IconNames.FILTER}
                          text="Filter"
                          onClick={() => onFilter(column)}
                        />
                      )}
                      <MenuItem
                        icon={IconNames.EYE_OPEN}
                        text="Show"
                        onClick={() => onShowColumn(column)}
                      />
                      <MenuDivider />
                    </>
                  )}
                  <MenuItem
                    icon={IconNames.EDIT}
                    text="Edit"
                    onClick={() =>
                      setColumnEditorOpenOn({
                        expression: querySource.getSourceExpressionForColumn(columnName),
                      })
                    }
                  />
                  <MenuItem
                    icon={IconNames.DUPLICATE}
                    text="Duplicate"
                    onClick={() =>
                      setColumnEditorOpenOn({
                        columnToDuplicate: columnName,
                        expression: querySource
                          .getSourceExpressionForColumn(columnName)
                          .as(querySource.getAvailableName(columnName)),
                      })
                    }
                  />
                  <MenuItem
                    icon={IconNames.TRASH}
                    text="Delete"
                    intent={Intent.DANGER}
                    onClick={() => onQueryChange(querySource.deleteColumn(columnName), undefined)}
                  />
                </Menu>
              }
            >
              <div
                className={Classes.MENU_ITEM}
                draggable={!isNestedColumn}
                onDragStart={e => {
                  e.dataTransfer.effectAllowed = 'all';
                  DragHelper.dragColumn = column;
                  DragHelper.createDragGhost(e.dataTransfer, columnName);
                }}
              >
                <Icon
                  className={Classes.MENU_ITEM_ICON}
                  icon={columnToIcon(column) || IconNames.BLANK}
                  data-tooltip={`${columnName}\nSQL type: ${column.sqlType}\nNative type: ${column.nativeType}`}
                />
                <div className={classNames(Classes.FILL, Classes.TEXT_OVERFLOW_ELLIPSIS)}>
                  {columnName}
                </div>
              </div>
            </Popover>
          );
        })}
      </div>
      <div className="list-header measure-list-header">
        Measures
        <ButtonGroup className="header-buttons" minimal>
          <Button
            icon={IconNames.PLUS}
            title="Add measure"
            onClick={() => setMeasureEditorOpenOn({})}
          />
        </ButtonGroup>
      </div>
      <div className="measure-resource-list">
        {filterMap(querySource.measures, (measure, i) => {
          const measureName = measure.name;
          if (!caseInsensitiveContains(measureName, columnSearch)) return;
          return (
            <Popover
              className="measure-resource"
              key={i}
              position="right"
              content={
                <Menu>
                  <MenuItem
                    icon={IconNames.EYE_OPEN}
                    text="Show"
                    onClick={() => onShowMeasure(measure)}
                  />
                  <MenuDivider />
                  <MenuItem
                    icon={IconNames.EDIT}
                    text="Edit"
                    onClick={() =>
                      setMeasureEditorOpenOn({
                        measure,
                      })
                    }
                  />
                  <MenuItem
                    icon={IconNames.DUPLICATE}
                    text="Duplicate"
                    onClick={() =>
                      setMeasureEditorOpenOn({
                        measureToDuplicate: measureName,
                        measure: measure.changeAs(querySource.getAvailableName(measureName)),
                      })
                    }
                  />
                  <MenuItem
                    icon={IconNames.TRASH}
                    text="Delete"
                    intent={Intent.DANGER}
                    onClick={() => onQueryChange(querySource.deleteMeasure(measureName), undefined)}
                  />
                </Menu>
              }
            >
              <div
                className={Classes.MENU_ITEM}
                draggable
                onDragStart={e => {
                  e.dataTransfer.effectAllowed = 'all';
                  DragHelper.dragMeasure = measure.toAggregateBasedMeasure();
                  DragHelper.createDragGhost(e.dataTransfer, measure.name);
                }}
              >
                <Icon className={Classes.MENU_ITEM_ICON} icon={IconNames.PULSE} />
                <div className={classNames(Classes.FILL, Classes.TEXT_OVERFLOW_ELLIPSIS)}>
                  {measureName}
                </div>
              </div>
            </Popover>
          );
        })}
      </div>
      {columnEditorOpenOn && (
        <ColumnDialog
          initExpression={columnEditorOpenOn.expression}
          columnToDuplicate={columnEditorOpenOn.columnToDuplicate}
          onApply={onQueryChange}
          querySource={querySource}
          runSqlQuery={runSqlQuery}
          onClose={() => setColumnEditorOpenOn(undefined)}
        />
      )}
      {nestedColumnEditorOpenOn && (
        <NestedColumnDialog
          nestedColumn={nestedColumnEditorOpenOn}
          onApply={newQuery => onQueryChange(newQuery, undefined)}
          querySource={querySource}
          runSqlQuery={runSqlQuery}
          onClose={() => setNestedColumnEditorOpenOn(undefined)}
        />
      )}
      {measureEditorOpenOn && (
        <MeasureDialog
          initMeasure={measureEditorOpenOn.measure}
          measureToDuplicate={measureEditorOpenOn.measureToDuplicate}
          onApply={onQueryChange}
          querySource={querySource}
          runSqlQuery={runSqlQuery}
          onClose={() => setMeasureEditorOpenOn(undefined)}
        />
      )}
    </div>
  );
};
