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

import { HTMLSelect, IconName, ITreeNode, Menu, MenuItem, Position, Tree } from '@blueprintjs/core';
import { Popover } from '@blueprintjs/core/lib/cjs';
import { IconNames } from '@blueprintjs/icons';
import React, { ChangeEvent } from 'react';

import { Loader } from '../../../components';
import { copyAndAlert, escapeSqlIdentifier, groupBy } from '../../../utils';
import { ColumnMetadata } from '../../../utils/column-metadata';

import './column-tree.scss';

function handleTableClick(
  tableSchema: string,
  nodeData: ITreeNode,
  onQueryStringChange: (queryString: string) => void,
): void {
  let columns: string[];
  if (nodeData.childNodes) {
    columns = nodeData.childNodes.map(child => escapeSqlIdentifier(String(child.label)));
  } else {
    columns = ['*'];
  }
  if (tableSchema === 'druid') {
    onQueryStringChange(`SELECT ${columns.join(', ')}
FROM ${escapeSqlIdentifier(String(nodeData.label))}
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);
  } else {
    onQueryStringChange(`SELECT ${columns.join(', ')}
FROM ${tableSchema}.${nodeData.label}`);
  }
}

function handleColumnClick(
  columnSchema: string,
  columnTable: string,
  nodeData: ITreeNode,
  onQueryStringChange: (queryString: string) => void,
): void {
  if (columnSchema === 'druid') {
    if (nodeData.icon === IconNames.TIME) {
      onQueryStringChange(`SELECT
  TIME_FLOOR(${escapeSqlIdentifier(String(nodeData.label))}, 'PT1H') AS "Time",
  COUNT(*) AS "Count"
FROM ${escapeSqlIdentifier(columnTable)}
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1
ORDER BY "Time" ASC`);
    } else {
      onQueryStringChange(`SELECT
  "${nodeData.label}",
  COUNT(*) AS "Count"
FROM ${escapeSqlIdentifier(columnTable)}
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1
ORDER BY "Count" DESC`);
    }
  } else {
    onQueryStringChange(`SELECT
  ${escapeSqlIdentifier(String(nodeData.label))},
  COUNT(*) AS "Count"
FROM ${columnSchema}.${columnTable}
GROUP BY 1
ORDER BY "Count" DESC`);
  }
}

export interface ColumnTreeProps {
  columnMetadataLoading: boolean;
  columnMetadata?: ColumnMetadata[];
  onQueryStringChange: (queryString: string) => void;
  defaultSchema?: string;
  defaultTable?: string;
  addToGroupBy: (columnName: string) => void;
  hasGroupBy?: boolean;
}

export interface ColumnTreeState {
  prevColumnMetadata?: ColumnMetadata[];
  columnTree?: ITreeNode[];
  selectedTreeIndex: number;
  expandedNode: number;
}

export class ColumnTree extends React.PureComponent<ColumnTreeProps, ColumnTreeState> {
  static getDerivedStateFromProps(props: ColumnTreeProps, state: ColumnTreeState) {
    const { columnMetadata, defaultSchema, defaultTable } = props;

    if (columnMetadata && columnMetadata !== state.prevColumnMetadata) {
      const columnTree = groupBy(
        columnMetadata,
        r => r.TABLE_SCHEMA,
        (metadata, schema): ITreeNode => ({
          id: schema,
          label: schema,
          childNodes: groupBy(
            metadata,
            r => r.TABLE_NAME,
            (metadata, table) => ({
              id: table,
              icon: IconNames.TH,
              label: (
                <Popover
                  boundary={'window'}
                  position={Position.RIGHT}
                  content={
                    <Menu>
                      <MenuItem
                        icon={IconNames.FULLSCREEN}
                        text={`Show: ${table}`}
                        onClick={() => {
                          handleTableClick(
                            schema,
                            {
                              id: table,
                              icon: IconNames.TH,
                              label: table,
                              childNodes: metadata.map(columnData => ({
                                id: columnData.COLUMN_NAME,
                                icon: ColumnTree.dataTypeToIcon(columnData.DATA_TYPE),
                                label: columnData.COLUMN_NAME,
                              })),
                            },
                            props.onQueryStringChange,
                          );
                        }}
                      />
                      <MenuItem
                        icon={IconNames.CLIPBOARD}
                        text={`Copy: ${table}`}
                        onClick={() => {
                          copyAndAlert(table, `${table} query copied to clipboard`);
                        }}
                      />
                    </Menu>
                  }
                >
                  <div>{table}</div>
                </Popover>
              ),
              childNodes: metadata.map(columnData => ({
                id: columnData.COLUMN_NAME,
                icon: ColumnTree.dataTypeToIcon(columnData.DATA_TYPE),
                label: (
                  <Popover
                    boundary={'window'}
                    position={Position.RIGHT}
                    content={
                      <Menu>
                        <MenuItem
                          icon={IconNames.FULLSCREEN}
                          text={`Show: ${columnData.COLUMN_NAME}`}
                          onClick={() => {
                            handleColumnClick(
                              schema,
                              table,
                              {
                                id: columnData.COLUMN_NAME,
                                icon: ColumnTree.dataTypeToIcon(columnData.DATA_TYPE),
                                label: columnData.COLUMN_NAME,
                              },
                              props.onQueryStringChange,
                            );
                          }}
                        />
                        {props.hasGroupBy && (
                          <MenuItem
                            icon={IconNames.GROUP_OBJECTS}
                            text={`Add to group by: ${columnData.COLUMN_NAME}`}
                            onClick={() => props.addToGroupBy(columnData.COLUMN_NAME)}
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
                    }
                  >
                    <div>{columnData.COLUMN_NAME}</div>
                  </Popover>
                ),
              })),
            }),
          ),
        }),
      );

      let selectedTreeIndex = -1;
      let expandedNode = -1;
      if (defaultSchema && columnTree) {
        selectedTreeIndex = columnTree
          .map(function(x) {
            return x.id;
          })
          .indexOf(defaultSchema);
      }
      if (selectedTreeIndex > -1) {
        const treeNodes = columnTree[selectedTreeIndex].childNodes;
        if (treeNodes) {
          if (defaultTable) {
            expandedNode = treeNodes
              .map(node => {
                return node.id;
              })
              .indexOf(defaultTable);
          }
        }
      }
      return {
        prevColumnMetadata: columnMetadata,
        columnTree,
        selectedTreeIndex,
        expandedNode,
      };
    }
    return null;
  }

  static dataTypeToIcon(dataType: string): IconName {
    switch (dataType) {
      case 'TIMESTAMP':
        return IconNames.TIME;
      case 'VARCHAR':
        return IconNames.FONT;
      case 'BIGINT':
        return IconNames.NUMERICAL;
      default:
        return IconNames.HELP;
    }
  }

  constructor(props: ColumnTreeProps, context: any) {
    super(props, context);
    this.state = {
      selectedTreeIndex: -1,
      expandedNode: -1,
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

  private handleSchemaSelectorChange = (e: ChangeEvent<HTMLSelectElement>) => {
    this.setState({ selectedTreeIndex: Number(e.target.value), expandedNode: -1 });
  };

  render(): JSX.Element | null {
    const { columnMetadataLoading } = this.props;
    if (columnMetadataLoading) {
      return (
        <div className="column-tree">
          <Loader loading />
        </div>
      );
    }

    const { columnTree, selectedTreeIndex, expandedNode } = this.state;
    if (!columnTree) return null;
    const currentSchemaSubtree =
      columnTree[selectedTreeIndex > -1 ? selectedTreeIndex : 0].childNodes;
    if (!currentSchemaSubtree) return null;

    if (expandedNode > -1) {
      currentSchemaSubtree[expandedNode].isExpanded = true;
    }
    return (
      <div className="column-tree">
        {this.renderSchemaSelector()}
        <div className="tree-container">
          <Tree
            contents={currentSchemaSubtree}
            onNodeClick={() => this.setState({ expandedNode: -1 })}
            onNodeCollapse={this.handleNodeCollapse}
            onNodeExpand={this.handleNodeExpand}
          />
        </div>
      </div>
    );
  }

  private handleNodeCollapse = (nodeData: ITreeNode) => {
    this.setState({ expandedNode: -1 });
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
    this.setState({ columnTree: columnTree.slice() });
  }
}
