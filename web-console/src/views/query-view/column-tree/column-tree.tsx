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

import { HTMLSelect, IconName, ITreeNode, Tree } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { ChangeEvent } from 'react';

import { Loader } from '../../../components';
import { groupBy } from '../../../utils';
import { ColumnMetadata } from '../../../utils/column-metadata';

import './column-tree.scss';

export interface ColumnTreeProps {
  columnMetadataLoading: boolean;
  columnMetadata?: ColumnMetadata[];
  onQueryStringChange: (queryString: string) => void;
}

export interface ColumnTreeState {
  prevColumnMetadata?: ColumnMetadata[];
  columnTree?: ITreeNode[];
  selectedTreeIndex: number;
}

export class ColumnTree extends React.PureComponent<ColumnTreeProps, ColumnTreeState> {
  static getDerivedStateFromProps(props: ColumnTreeProps, state: ColumnTreeState) {
    const { columnMetadata } = props;

    if (columnMetadata && columnMetadata !== state.prevColumnMetadata) {
      return {
        prevColumnMetadata: columnMetadata,
        columnTree: groupBy(
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
                  label: columnData.COLUMN_NAME,
                })),
              }),
            ),
          }),
        ),
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
      selectedTreeIndex: 0,
    };
  }

  renderSchemaSelector() {
    const { columnTree, selectedTreeIndex } = this.state;
    if (!columnTree) return null;

    return (
      <HTMLSelect
        className="schema-selector"
        value={selectedTreeIndex}
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
    this.setState({ selectedTreeIndex: Number(e.target.value) });
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

    const { columnTree, selectedTreeIndex } = this.state;
    if (!columnTree) return null;
    const currentSchemaSubtree = columnTree[selectedTreeIndex].childNodes;
    if (!currentSchemaSubtree) return null;

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
    if (!columnTree) return;

    const selectedNode = columnTree[selectedTreeIndex];
    switch (nodePath.length) {
      case 1: // Datasource
        const tableSchema = selectedNode.label;
        let columns: string[];
        if (nodeData.childNodes) {
          columns = nodeData.childNodes.map(child => String(child.label));
        } else {
          columns = ['*'];
        }
        if (tableSchema === 'druid') {
          onQueryStringChange(`SELECT ${columns.join(', ')}
FROM "${nodeData.label}"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);
        } else {
          onQueryStringChange(`SELECT ${columns.join(', ')}
FROM ${tableSchema}.${nodeData.label}`);
        }
        break;

      case 2: // Column
        const schemaNode = selectedNode;
        const columnSchema = schemaNode.label;
        const columnTable = schemaNode.childNodes ? schemaNode.childNodes[nodePath[0]].label : '?';
        if (columnSchema === 'druid') {
          if (nodeData.icon === IconNames.TIME) {
            onQueryStringChange(`SELECT TIME_FLOOR("${nodeData.label}", 'PT1H') AS "Time", COUNT(*) AS "Count"
FROM "${columnTable}"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1
ORDER BY "Time" ASC`);
          } else {
            onQueryStringChange(`SELECT "${nodeData.label}", COUNT(*) AS "Count"
FROM "${columnTable}"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1
ORDER BY "Count" DESC`);
          }
        } else {
          onQueryStringChange(`SELECT "${nodeData.label}", COUNT(*) AS "Count"
FROM ${columnSchema}.${columnTable}
GROUP BY 1
ORDER BY "Count" DESC`);
        }
        break;
    }
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
    this.setState({ columnTree: columnTree.slice() });
  }
}
