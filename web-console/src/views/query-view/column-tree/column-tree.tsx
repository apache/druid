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

import { HTMLSelect, Menu, MenuItem, Position, Tree, TreeNodeInfo } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import {
  SqlComparison,
  SqlExpression,
  SqlFunction,
  SqlJoinPart,
  SqlQuery,
  SqlRef,
  SqlTableRef,
} from 'druid-query-toolkit';
import React, { ChangeEvent } from 'react';

import { Deferred, Loader } from '../../../components';
import {
  ColumnMetadata,
  copyAndAlert,
  dataTypeToIcon,
  groupBy,
  oneOf,
  prettyPrintSql,
} from '../../../utils';

import { NumberMenuItems, StringMenuItems, TimeMenuItems } from './column-tree-menu';

import './column-tree.scss';

const COUNT_STAR = SqlFunction.COUNT_STAR.as('Count');

function getCountExpression(columnNames: string[]): SqlExpression {
  for (const columnName of columnNames) {
    if (columnName === 'count' || columnName === '__count') {
      return SqlFunction.simple('SUM', [SqlRef.column(columnName)]).as('Count');
    }
  }
  return COUNT_STAR;
}

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
  defaultWhere: SqlExpression | undefined;
  onQueryChange: (query: SqlQuery, run: boolean) => void;
}

function handleColumnShow(options: HandleColumnClickOptions): void {
  const {
    columnSchema,
    columnTable,
    columnName,
    columnType,
    parsedQuery,
    defaultWhere,
    onQueryChange,
  } = options;

  let from: SqlExpression;
  let where: SqlExpression | undefined;
  let aggregates: SqlExpression[] = [];
  if (parsedQuery && parsedQuery.getFirstTableName() === columnTable) {
    from = parsedQuery.getFirstFromExpression()!;
    where = parsedQuery.getWhereExpression();
    aggregates = parsedQuery.getAggregateSelectExpressions();
  } else if (columnSchema === 'druid') {
    from = SqlTableRef.create(columnTable);
    where = defaultWhere;
  } else {
    from = SqlTableRef.create(columnTable, columnSchema);
  }

  if (!aggregates.length) {
    aggregates.push(COUNT_STAR);
  }

  const columnRef = SqlRef.column(columnName);
  let query: SqlQuery;
  if (columnSchema === 'druid' && columnType === 'TIMESTAMP') {
    query = TIME_QUERY.fillPlaceholders([columnRef, from]) as SqlQuery;
  } else {
    query = STRING_QUERY.fillPlaceholders([columnRef, from]) as SqlQuery;
  }

  let newSelectExpressions = query.selectExpressions;
  if (newSelectExpressions) {
    for (const aggregate of aggregates) {
      newSelectExpressions = newSelectExpressions.append(aggregate);
    }
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
  defaultWhere?: SqlExpression;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
  defaultSchema?: string;
  defaultTable?: string;
  highlightTable?: string;
}

export interface ColumnTreeState {
  prevColumnMetadata?: readonly ColumnMetadata[];
  columnTree?: TreeNodeInfo[];
  currentSchemaSubtree?: TreeNodeInfo[];
  selectedTreeIndex: number;
}

export function getJoinColumns(parsedQuery: SqlQuery, _table: string) {
  let lookupColumn: string | undefined;
  let originalTableColumn: string | undefined;
  if (parsedQuery.fromClause && parsedQuery.fromClause.joinParts) {
    const firstOnExpression = parsedQuery.fromClause.joinParts.first().onExpression;
    if (firstOnExpression instanceof SqlComparison && firstOnExpression.op === '=') {
      const { lhs, rhs } = firstOnExpression;
      if (lhs instanceof SqlRef && lhs.getNamespace() === 'lookup') {
        lookupColumn = lhs.getColumn();
      }
      if (rhs instanceof SqlRef) {
        originalTableColumn = rhs.getColumn();
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
    const {
      columnMetadata,
      defaultSchema,
      defaultTable,
      defaultWhere,
      onQueryChange,
      highlightTable,
    } = props;

    if (columnMetadata && columnMetadata !== state.prevColumnMetadata) {
      const columnTree = groupBy(
        columnMetadata,
        r => r.TABLE_SCHEMA,
        (metadata, schemaName): TreeNodeInfo => ({
          id: schemaName,
          label: schemaName,
          childNodes: groupBy(
            metadata,
            r => r.TABLE_NAME,
            (metadata, tableName): TreeNodeInfo => ({
              id: tableName,
              icon: IconNames.TH,
              className: tableName === highlightTable ? 'highlight' : undefined,
              label: (
                <Popover2
                  position={Position.RIGHT}
                  content={
                    <Deferred
                      content={() => {
                        const parsedQuery = props.getParsedQuery();
                        const tableRef = SqlTableRef.create(tableName);
                        const prettyTableRef = prettyPrintSql(tableRef);
                        const countExpression = getCountExpression(
                          metadata.map(child => child.COLUMN_NAME),
                        );

                        const getQueryOnTable = () => {
                          return SqlQuery.create(
                            SqlTableRef.create(
                              tableName,
                              schemaName === 'druid' ? undefined : schemaName,
                            ),
                          );
                        };

                        const getWhere = (defaultToAllTime = false) => {
                          if (parsedQuery && parsedQuery.getFirstTableName() === tableName) {
                            return parsedQuery.getWhereExpression();
                          } else if (schemaName === 'druid') {
                            return defaultToAllTime ? undefined : defaultWhere;
                          } else {
                            return;
                          }
                        };

                        return (
                          <Menu>
                            <MenuItem
                              icon={IconNames.FULLSCREEN}
                              text={`SELECT ...columns... FROM ${tableName}`}
                              onClick={() => {
                                onQueryChange(
                                  getQueryOnTable()
                                    .changeSelectExpressions(
                                      metadata
                                        .map(child => child.COLUMN_NAME)
                                        .map(columnName => SqlRef.column(columnName)),
                                    )
                                    .changeWhereExpression(getWhere()),
                                  true,
                                );
                              }}
                            />
                            <MenuItem
                              icon={IconNames.FULLSCREEN}
                              text={`SELECT * FROM ${tableName}`}
                              onClick={() => {
                                onQueryChange(
                                  getQueryOnTable().changeWhereExpression(getWhere()),
                                  true,
                                );
                              }}
                            />
                            <MenuItem
                              icon={IconNames.FULLSCREEN}
                              text={`SELECT ${countExpression} FROM ${tableName}`}
                              onClick={() => {
                                onQueryChange(
                                  getQueryOnTable()
                                    .changeSelect(0, countExpression)
                                    .changeGroupByExpressions([])
                                    .changeWhereExpression(getWhere(true)),
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
                                  text="Left join"
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
                                            SqlTableRef.create(tableName, schemaName),
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
                                  text="Inner join"
                                  onClick={() => {
                                    const { lookupColumn, originalTableColumn } = getJoinColumns(
                                      parsedQuery,
                                      tableName,
                                    );
                                    onQueryChange(
                                      parsedQuery.addJoin(
                                        SqlJoinPart.create(
                                          'INNER',
                                          SqlTableRef.create(tableName, schemaName),
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
                                  text="Remove join"
                                  onClick={() => onQueryChange(parsedQuery.removeAllJoins())}
                                />
                              )}
                            {parsedQuery &&
                              parsedQuery.hasGroupBy() &&
                              parsedQuery.getFirstTableName() === tableName && (
                                <MenuItem
                                  icon={IconNames.FUNCTION}
                                  text="Aggregate COUNT(*)"
                                  onClick={() =>
                                    onQueryChange(parsedQuery.addSelect(COUNT_STAR), true)
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
                </Popover2>
              ),
              childNodes: metadata.map(
                (columnData): TreeNodeInfo => ({
                  id: columnData.COLUMN_NAME,
                  icon: dataTypeToIcon(columnData.DATA_TYPE),
                  label: (
                    <Popover2
                      position={Position.RIGHT}
                      autoFocus={false}
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
                                    handleColumnShow({
                                      columnSchema: schemaName,
                                      columnTable: tableName,
                                      columnName: columnData.COLUMN_NAME,
                                      columnType: columnData.DATA_TYPE,
                                      parsedQuery,
                                      defaultWhere,
                                      onQueryChange: onQueryChange,
                                    });
                                  }}
                                />
                                {parsedQuery &&
                                  oneOf(columnData.DATA_TYPE, 'BIGINT', 'FLOAT', 'DOUBLE') && (
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
                    </Popover2>
                  ),
                }),
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

  private renderSchemaSelector() {
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

  private readonly handleSchemaSelectorChange = (e: ChangeEvent<HTMLSelectElement>): void => {
    const { columnTree } = this.state;
    if (!columnTree) return;

    const selectedTreeIndex = Number(e.target.value);

    const currentSchemaSubtree =
      columnTree[selectedTreeIndex > -1 ? selectedTreeIndex : 0].childNodes;

    this.setState({
      selectedTreeIndex: Number(e.target.value),
      currentSchemaSubtree: currentSchemaSubtree,
    });
  };

  private readonly handleNodeCollapse = (nodeData: TreeNodeInfo) => {
    nodeData.isExpanded = false;
    this.forceUpdate();
  };

  private readonly handleNodeExpand = (nodeData: TreeNodeInfo) => {
    nodeData.isExpanded = true;
    this.forceUpdate();
  };

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
