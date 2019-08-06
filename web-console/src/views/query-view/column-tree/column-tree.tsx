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

import { HTMLSelect, IconName, Intent, ITreeNode, Tree } from '@blueprintjs/core';
import { Popover } from '@blueprintjs/core/lib/cjs';
import { Position } from '@blueprintjs/core/lib/esm/common/position';
import { IconNames } from '@blueprintjs/icons';
import copy = require('copy-to-clipboard');
import React, { ChangeEvent } from 'react';

import { Loader } from '../../../components';
import { AppToaster } from '../../../singletons/toaster';
import { escapeSqlIdentifier, groupBy } from '../../../utils';
import { basicActionsToMenu } from '../../../utils/basic-action';
import { ColumnMetadata } from '../../../utils/column-metadata';

import './column-tree.scss';

function handleNodeClick(nodeData: ITreeNode, nodePath: number[]): void {
  const { onQueryStringChange } = this.props;
  const { columnTree, selectedTreeIndex } = this.state;

  console.log(nodeData, nodePath);

  const selectedNode = columnTree[selectedTreeIndex];
  switch (nodePath.length) {
    case 1: // Datasource
      const tableSchema = selectedNode.label;
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
      break;

    case 2: // Column
      const schemaNode = selectedNode;
      const columnSchema = schemaNode.label;
      const columnTable = schemaNode.childNodes
        ? String(schemaNode.childNodes[nodePath[0]].label)
        : '?';
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
      break;
  }
}

export interface ColumnTreeProps {
  columnMetadataLoading: boolean;
  columnMetadata?: ColumnMetadata[];
  onQueryStringChange: (queryString: string) => void;
  defaultSchema?: string;
  defaultTable?: string;
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
              label: table,
              childNodes: metadata.map(columnData => ({
                id: columnData.COLUMN_NAME,
                icon: ColumnTree.dataTypeToIcon(columnData.DATA_TYPE),
                label: (
                  <Popover
                    position={Position.LEFT}
                    content={basicActionsToMenu([
                      {
                        icon: IconNames.CLIPBOARD,
                        title: `Copy ${columnData.COLUMN_NAME}`,
                        onAction: () => {
                          copy(columnData.COLUMN_NAME, { format: 'text/plain' });
                          AppToaster.show({
                            message: `${columnData.COLUMN_NAME}' copied to clipboard`,
                            intent: Intent.SUCCESS,
                          });
                        },
                      },
                      {
                        icon: IconNames.FULLSCREEN,
                        title: `Show ${columnData.COLUMN_NAME}`,
                        onAction: () => {
                          handleNodeClick(columnData.COLUMN_NAME);
                        },
                      },
                    ])}
                  >
                    {columnData.COLUMN_NAME}
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
    console.log(currentSchemaSubtree);
    return (
      <div className="column-tree">
        {this.renderSchemaSelector()}
        <div className="tree-container">
          <Tree
            contents={currentSchemaSubtree}
            onNodeClick={this.handleNodeClick}
            onNodeCollapse={this.handleNodeCollapse}
            onNodeExpand={this.handleNodeExpand}
          />
        </div>
      </div>
    );
  }

  private handleNodeClick = (nodeData: ITreeNode, nodePath: number[]) => {
    const { onQueryStringChange } = this.props;
    const { columnTree, selectedTreeIndex } = this.state;
    this.setState({ expandedNode: -1 });
    if (!columnTree) return;

    const selectedNode = columnTree[selectedTreeIndex];

    console.log(selectedNode);

    switch (nodePath.length) {
      case 1: // Datasource
        const tableSchema = selectedNode.label;
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
        break;

      case 2: // Column
        const schemaNode = selectedNode;
        const columnSchema = schemaNode.label;
        const columnTable = schemaNode.childNodes
          ? String(schemaNode.childNodes[nodePath[0]].label)
          : '?';
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
        break;
    }
  };

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
