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

import { HTMLSelect, ITreeNode, Menu, MenuItem, Popover, Position, Tree } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import {
  SqlAlias,
  SqlComparison,
  SqlExpression,
  SqlFunction,
  SqlJoinPart,
  SqlQuery,
  SqlRef,
} from 'druid-query-toolkit';
import React, { ChangeEvent } from 'react';

import { Loader } from '../../../components';
import { Deferred } from '../../../components/deferred/deferred';
import { copyAndAlert, groupBy, prettyPrintSql } from '../../../utils';
import { ColumnMetadata } from '../../../utils/column-metadata';
import { dataTypeToIcon } from '../query-utils';

import { NumberMenuItems, StringMenuItems, TimeMenuItems } from './column-tree-menu';

import './column-tree.scss';

const LAST_DAY = SqlExpression.parse(`__time >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);
const COUNT_STAR = SqlFunction.COUNT_STAR.as('Count');

const STRING_QUERY = SqlQuery.parse(`SELECT
  ?
FROM ?
GROUP BY 1
ORDER BY 2 DESC`);

const TIME_QUERY = SqlQuery.parse(`SELECT
  TIME_FLOOR(?, 'PT1H') AS "Time"
FROM ?
GROUP BY 1
ORDER BY 1 ASC`);

interface HandleColumnClickOptions {
  columnSchema: string;
  columnTable: string;
  columnName: string;
  columnType: string;
  parsedQuery: SqlQuery | undefined;
  onQueryChange: (query: SqlQuery, run: boolean) => void;
}

function handleColumnClick(options: HandleColumnClickOptions): void {
  const { columnSchema, columnTable, columnName, columnType, parsedQuery, onQueryChange } = options;

  let query: SqlQuery;
  const columnRef = SqlRef.column(columnName);
  if (columnSchema === 'druid') {
    if (columnType === 'TIMESTAMP') {
      query = TIME_QUERY.fillPlaceholders([columnRef, SqlRef.table(columnTable)]) as SqlQuery;
    } else {
      query = STRING_QUERY.fillPlaceholders([columnRef, SqlRef.table(columnTable)]) as SqlQuery;
    }
  } else {
    query = STRING_QUERY.fillPlaceholders([
      columnRef,
      SqlRef.table(columnTable, columnSchema),
    ]) as SqlQuery;
  }

  let where: SqlExpression | undefined;
  let aggregates: SqlAlias[] = [];
  if (parsedQuery && parsedQuery.getFirstTableName() === columnTable) {
    where = parsedQuery.getWhereExpression();
    aggregates = parsedQuery.getAggregateSelectExpressions();
  } else if (columnSchema === 'druid') {
    where = LAST_DAY;
  }
  if (!aggregates.length) {
    aggregates.push(COUNT_STAR);
  }

  let newSelectExpressions = query.selectExpressions;
  for (const aggregate of aggregates) {
    newSelectExpressions = newSelectExpressions.addLast(aggregate);
  }

  onQueryChange(
    query.changeSelectExpressions(newSelectExpressions).changeWhereExpression(where),
    true,
  );
}

export interface ColumnTreeProps {
  columnMetadataLoading: boolean;
  columnMetadata?: readonly ColumnMetadata[];
  getParsedQuery: () => SqlQuery | undefined;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
  defaultSchema?: string;
  defaultTable?: string;
}

export interface ColumnTreeState {
  prevColumnMetadata?: readonly ColumnMetadata[];
  columnTree?: ITreeNode[];
  currentSchemaSubtree?: ITreeNode[];
  selectedTreeIndex: number;
}

export function getJoinColumns(parsedQuery: SqlQuery, _table: string) {
  let lookupColumn: string | undefined;
  let originalTableColumn: string | undefined;
  if (parsedQuery.fromClause && parsedQuery.fromClause.joinParts) {
    const firstOnExpression = parsedQuery.fromClause.joinParts.first().onExpression;
    if (firstOnExpression instanceof SqlComparison && firstOnExpression.op === '=') {
      const { lhs, rhs } = firstOnExpression;
      if (lhs instanceof SqlRef && lhs.namespace === 'lookup') {
        lookupColumn = lhs.column;
      }
      if (rhs instanceof SqlRef) {
        originalTableColumn = rhs.column;
      }
    }
  }

  return {
    lookupColumn: lookupColumn || 'k',
    originalTableColumn: originalTableColumn || 'XXX',
  };
}

export class ColumnTree extends React.PureComponent<ColumnTreeProps, ColumnTreeState> {
  static getDerivedStateFromProps(props: ColumnTreeProps, state: ColumnTreeState) {
    const { columnMetadata, defaultSchema, defaultTable, onQueryChange } = props;

    if (columnMetadata && columnMetadata !== state.prevColumnMetadata) {
      const columnTree = groupBy(
        columnMetadata,
        r => r.TABLE_SCHEMA,
        (metadata, schemaName): ITreeNode => ({
          id: schemaName,
          label: schemaName,
          childNodes: groupBy(
            metadata,
            r => r.TABLE_NAME,
            (metadata, tableName): ITreeNode => ({
              id: tableName,
              icon: IconNames.TH,
              label: (
                <Popover
                  boundary={'window'}
                  position={Position.RIGHT}
                  content={
                    <Deferred
                      content={() => {
                        const parsedQuery = props.getParsedQuery();
                        const tableRef = SqlRef.table(tableName).as();
                        const prettyTableRef = prettyPrintSql(tableRef);
                        return (
                          <Menu>
                            <MenuItem
                              icon={IconNames.FULLSCREEN}
                              text={`SELECT ... FROM ${tableName}`}
                              onClick={() => {
                                const tableRef = SqlRef.table(
                                  tableName,
                                  schemaName === 'druid' ? undefined : schemaName,
                                );

                                let where: SqlExpression | undefined;
                                if (parsedQuery && parsedQuery.getFirstTableName() === tableName) {
                                  where = parsedQuery.getWhereExpression();
                                } else if (schemaName === 'druid') {
                                  where = LAST_DAY;
                                }

                                onQueryChange(
                                  SqlQuery.create(tableRef)
                                    .changeSelectExpressions(
                                      metadata.map(child => SqlRef.column(child.COLUMN_NAME).as()),
                                    )
                                    .changeWhereExpression(where),
                                  true,
                                );
                              }}
                            />
                            {parsedQuery && parsedQuery.getFirstTableName() !== tableName && (
                              <MenuItem
                                icon={IconNames.EXCHANGE}
                                text={`Replace FROM with: ${prettyTableRef}`}
                                onClick={() => {
                                  onQueryChange(
                                    parsedQuery.changeFromExpressions([tableRef]),
                                    true,
                                  );
                                }}
                              />
                            )}
                            {parsedQuery && schemaName === 'lookup' && (
                              <MenuItem
                                popoverProps={{ openOnTargetFocus: false }}
                                icon={IconNames.JOIN_TABLE}
                                text={parsedQuery.hasJoin() ? `Replace join` : `Join`}
                              >
                                <MenuItem
                                  icon={IconNames.LEFT_JOIN}
                                  text={`Left join`}
                                  onClick={() => {
                                    const { lookupColumn, originalTableColumn } = getJoinColumns(
                                      parsedQuery,
                                      tableName,
                                    );
                                    onQueryChange(
                                      parsedQuery
                                        .removeAllJoins()
                                        .addJoin(
                                          SqlJoinPart.create(
                                            'LEFT',
                                            SqlRef.column(tableName, schemaName).upgrade(),
                                            SqlRef.column(lookupColumn, tableName, 'lookup').equal(
                                              SqlRef.column(
                                                originalTableColumn,
                                                parsedQuery.getFirstTableName(),
                                              ),
                                            ),
                                          ),
                                        ),
                                      false,
                                    );
                                  }}
                                />
                                <MenuItem
                                  icon={IconNames.INNER_JOIN}
                                  text={`Inner join`}
                                  onClick={() => {
                                    const { lookupColumn, originalTableColumn } = getJoinColumns(
                                      parsedQuery,
                                      tableName,
                                    );
                                    onQueryChange(
                                      parsedQuery.addJoin(
                                        SqlJoinPart.create(
                                          'INNER',
                                          SqlRef.column(tableName, schemaName).upgrade(),
                                          SqlRef.column(lookupColumn, tableName, 'lookup').equal(
                                            SqlRef.column(
                                              originalTableColumn,
                                              parsedQuery.getFirstTableName(),
                                            ),
                                          ),
                                        ),
                                      ),
                                      false,
                                    );
                                  }}
                                />
                              </MenuItem>
                            )}
                            {parsedQuery &&
                              parsedQuery.hasJoin() &&
                              parsedQuery.getJoins()[0].table.toString() === tableName && (
                                <MenuItem
                                  icon={IconNames.EXCHANGE}
                                  text={`Remove join`}
                                  onClick={() => onQueryChange(parsedQuery.removeAllJoins())}
                                />
                              )}
                            {parsedQuery &&
                              parsedQuery.hasGroupBy() &&
                              parsedQuery.getFirstTableName() === tableName && (
                                <MenuItem
                                  icon={IconNames.FUNCTION}
                                  text={`Aggregate COUNT(*)`}
                                  onClick={() =>
                                    onQueryChange(parsedQuery.addSelectExpression(COUNT_STAR), true)
                                  }
                                />
                              )}
                            <MenuItem
                              icon={IconNames.CLIPBOARD}
                              text={`Copy: ${prettyTableRef}`}
                              onClick={() => {
                                copyAndAlert(
                                  tableRef.toString(),
                                  `${prettyTableRef} query copied to clipboard`,
                                );
                              }}
                            />
                          </Menu>
                        );
                      }}
                    />
                  }
                >
                  {tableName}
                </Popover>
              ),
              childNodes: metadata
                .map(
                  (columnData): ITreeNode => ({
                    id: columnData.COLUMN_NAME,
                    icon: dataTypeToIcon(columnData.DATA_TYPE),
                    label: (
                      <Popover
                        boundary={'window'}
                        position={Position.RIGHT}
                        autoFocus={false}
                        targetClassName={'bp3-popover-open'}
                        content={
                          <Deferred
                            content={() => {
                              const parsedQuery = props.getParsedQuery();
                              return (
                                <Menu>
                                  <MenuItem
                                    icon={IconNames.FULLSCREEN}
                                    text={`Show: ${columnData.COLUMN_NAME}`}
                                    onClick={() => {
                                      handleColumnClick({
                                        columnSchema: schemaName,
                                        columnTable: tableName,
                                        columnName: columnData.COLUMN_NAME,
                                        columnType: columnData.DATA_TYPE,
                                        parsedQuery,
                                        onQueryChange: onQueryChange,
                                      });
                                    }}
                                  />
                                  {parsedQuery &&
                                    (columnData.DATA_TYPE === 'BIGINT' ||
                                      columnData.DATA_TYPE === 'FLOAT') && (
                                      <NumberMenuItems
                                        table={tableName}
                                        schema={schemaName}
                                        columnName={columnData.COLUMN_NAME}
                                        parsedQuery={parsedQuery}
                                        onQueryChange={onQueryChange}
                                      />
                                    )}
                                  {parsedQuery && columnData.DATA_TYPE === 'VARCHAR' && (
                                    <StringMenuItems
                                      table={tableName}
                                      schema={schemaName}
                                      columnName={columnData.COLUMN_NAME}
                                      parsedQuery={parsedQuery}
                                      onQueryChange={onQueryChange}
                                    />
                                  )}
                                  {parsedQuery && columnData.DATA_TYPE === 'TIMESTAMP' && (
                                    <TimeMenuItems
                                      table={tableName}
                                      schema={schemaName}
                                      columnName={columnData.COLUMN_NAME}
                                      parsedQuery={parsedQuery}
                                      onQueryChange={onQueryChange}
                                    />
                                  )}
                                  <MenuItem
                                    icon={IconNames.CLIPBOARD}
                                    text={`Copy: ${columnData.COLUMN_NAME}`}
                                    onClick={() => {
                                      copyAndAlert(
                                        columnData.COLUMN_NAME,
                                        `${columnData.COLUMN_NAME} query copied to clipboard`,
                                      );
                                    }}
                                  />
                                </Menu>
                              );
                            }}
                          />
                        }
                      >
                        {columnData.COLUMN_NAME}
                      </Popover>
                    ),
                  }),
                )
                .sort((a, b) =>
                  String(a.id)
                    .toLowerCase()
                    .localeCompare(String(b.id).toLowerCase()),
                ),
            }),
          ),
        }),
      );

      let selectedTreeIndex = -1;
      let expandedNode = -1;
      if (defaultSchema && columnTree) {
        selectedTreeIndex = columnTree.findIndex(x => {
          return x.id === defaultSchema;
        });
      }

      if (selectedTreeIndex > -1) {
        const treeNodes = columnTree[selectedTreeIndex].childNodes;
        if (treeNodes) {
          if (defaultTable) {
            expandedNode = treeNodes.findIndex(node => {
              return node.id === defaultTable;
            });
          }
        }
      }

      if (!columnTree) return null;
      const currentSchemaSubtree =
        columnTree[selectedTreeIndex > -1 ? selectedTreeIndex : 0].childNodes;
      if (!currentSchemaSubtree) return null;

      if (expandedNode > -1) {
        currentSchemaSubtree[expandedNode].isExpanded = true;
      }

      return {
        prevColumnMetadata: columnMetadata,
        columnTree,
        selectedTreeIndex,
        currentSchemaSubtree,
      };
    }
    return null;
  }

  constructor(props: ColumnTreeProps, context: any) {
    super(props, context);
    this.state = {
      selectedTreeIndex: -1,
    };
  }

  renderSchemaSelector() {
    const { columnTree, selectedTreeIndex } = this.state;
    if (!columnTree) return null;

    return (
      <HTMLSelect
        className="schema-selector"
        value={selectedTreeIndex > -1 ? selectedTreeIndex : undefined}
        onChange={this.handleSchemaSelectorChange}
        fill
        minimal
        large
      >
        {columnTree.map((treeNode, i) => (
          <option key={i} value={i}>
            {treeNode.label}
          </option>
        ))}
      </HTMLSelect>
    );
  }

  private handleSchemaSelectorChange = (e: ChangeEvent<HTMLSelectElement>): void => {
    const { columnTree } = this.state;

    const selectedTreeIndex = Number(e.target.value);

    if (!columnTree) return;

    const currentSchemaSubtree =
      columnTree[selectedTreeIndex > -1 ? selectedTreeIndex : 0].childNodes;

    this.setState({
      selectedTreeIndex: Number(e.target.value),
      currentSchemaSubtree: currentSchemaSubtree,
    });
  };

  private handleNodeCollapse = (nodeData: ITreeNode) => {
    nodeData.isExpanded = false;
    this.bounceState();
  };

  private handleNodeExpand = (nodeData: ITreeNode) => {
    nodeData.isExpanded = true;
    this.bounceState();
  };

  bounceState() {
    const { columnTree } = this.state;
    if (!columnTree) return;
    this.setState(prevState => ({
      columnTree: prevState.columnTree ? prevState.columnTree.slice() : undefined,
    }));
  }

  render(): JSX.Element | null {
    const { columnMetadataLoading } = this.props;
    const { currentSchemaSubtree } = this.state;

    if (columnMetadataLoading) {
      return (
        <div className="column-tree">
          <Loader />
        </div>
      );
    }

    if (!currentSchemaSubtree) return null;

    return (
      <div className="column-tree">
        {this.renderSchemaSelector()}
        <div className="tree-container">
          <Tree
            contents={currentSchemaSubtree}
            onNodeCollapse={this.handleNodeCollapse}
            onNodeExpand={this.handleNodeExpand}
          />
        </div>
      </div>
    );
  }
}
