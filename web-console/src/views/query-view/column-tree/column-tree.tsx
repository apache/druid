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

import { IconName, ITreeNode, Tree } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { groupBy } from '../../../utils';
import { ColumnMetadata } from '../../../utils/column-metadata';

import './column-tree.scss';

export interface ColumnTreeProps extends React.Props<any> {
  columnMetadata: ColumnMetadata[] | null;
  onQueryStringChange: (queryString: string) => void;
}

export interface ColumnTreeState {
  prevColumnMetadata: ColumnMetadata[] | null;
  columnTree: ITreeNode[] | null;
}

export class ColumnTree extends React.PureComponent<ColumnTreeProps, ColumnTreeState> {

  static getDerivedStateFromProps(props: ColumnTreeProps, state: ColumnTreeState) {
    const { columnMetadata } = props;

    if (columnMetadata && columnMetadata !== state.prevColumnMetadata) {
      return {
        prevColumnMetadata: columnMetadata,
        columnTree: groupBy(
          columnMetadata,
          (r) => r.TABLE_SCHEMA,
          (metadata, schema): ITreeNode => ({
            id: schema,
            icon: IconNames.DATABASE,
            label: schema,
            isExpanded: schema === 'druid',
            childNodes: groupBy(
              metadata,
              (r) => r.TABLE_NAME,
              (metadata, table) => ({
                id: table,
                icon: IconNames.TH,
                label: table,
                childNodes: metadata.map(columnData => ({
                  id: columnData.COLUMN_NAME,
                  icon: ColumnTree.dataTypeToIcon(columnData.DATA_TYPE),
                  label: columnData.COLUMN_NAME
                }))
              })
            )
          })
        ).reverse()
      };
    }
    return null;
  }

  static dataTypeToIcon(dataType: string): IconName {
    switch (dataType) {
      case 'TIMESTAMP': return IconNames.TIME;
      case 'VARCHAR': return IconNames.FONT;
      case 'BIGINT': return IconNames.NUMERICAL;
      default: return IconNames.HELP;
    }
  }

  constructor(props: ColumnTreeProps, context: any) {
    super(props, context);
    this.state = {
      prevColumnMetadata: null,
      columnTree: null
    };
  }

  render() {
    const { columnTree } = this.state;
    if (!columnTree) return null;

    return <div className="column-tree">
      <Tree
        contents={columnTree}
        onNodeClick={this.handleNodeClick}
        onNodeCollapse={this.handleNodeCollapse}
        onNodeExpand={this.handleNodeExpand}
      />
    </div>;
  }

  private handleNodeClick = (nodeData: ITreeNode, nodePath: number[], e: React.MouseEvent<HTMLElement>) => {
    const { onQueryStringChange } = this.props;
    const { columnTree } = this.state;
    if (!columnTree) return;

    switch (nodePath.length) {
      case 1: // Schema
        nodeData.isExpanded = !nodeData.isExpanded;
        this.bounceState();
        break;

      case 2: // Datasource
        const tableSchema = columnTree[nodePath[0]].label;
        if (tableSchema === 'druid') {
          onQueryStringChange(`SELECT * FROM "${nodeData.label}"\nWHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);
        } else {
          onQueryStringChange(`SELECT * FROM ${tableSchema}.${nodeData.label}`);
        }
        break;

      case 3: // Column
        const schemaNode = columnTree[nodePath[0]];
        const columnSchema = schemaNode.label;
        const columnTable = schemaNode.childNodes ? schemaNode.childNodes[nodePath[1]].label : '?';
        if (columnSchema === 'druid') {
          onQueryStringChange(`SELECT "${nodeData.label}", COUNT(*) AS "Count"
FROM "${columnTable}"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY
GROUP BY 1
ORDER BY "Count" DESC`);
        } else {
          onQueryStringChange(`SELECT "${nodeData.label}", COUNT(*) AS "Count"
FROM ${columnSchema}.${columnTable}
GROUP BY 1
ORDER BY "Count" DESC`);
        }
        break;
    }
  }

  private handleNodeCollapse = (nodeData: ITreeNode) => {
    nodeData.isExpanded = false;
    this.bounceState();
  }

  private handleNodeExpand = (nodeData: ITreeNode) => {
    nodeData.isExpanded = true;
    this.bounceState();
  }

  bounceState() {
    const { columnTree } = this.state;
    if (!columnTree) return;
    this.setState({ columnTree: columnTree.slice() });
  }
}
