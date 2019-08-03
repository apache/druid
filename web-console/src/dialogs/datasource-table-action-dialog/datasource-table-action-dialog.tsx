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

import { IDialogProps } from '@blueprintjs/core';
import React from 'react';

import { ShowValue } from '../../components/show-value/show-value';
import { queryDruidSql, QueryManager } from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { ColumnMetadata } from '../../utils/column-metadata';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

interface DatasourceTableActionDialogProps extends IDialogProps {
  datasourceId?: string;
  actions: BasicAction[];
  onClose: () => void;
}

interface DatasourceTableActionDialogState {
  activeTab: 'dimensions';
  dimensions?: string;
  error?: string;
}

export class DatasourceTableActionDialog extends React.PureComponent<
  DatasourceTableActionDialogProps,
  DatasourceTableActionDialogState
> {
  private dimensionsQueryManager: QueryManager<null, string>;
  constructor(props: DatasourceTableActionDialogProps) {
    super(props);
    this.state = {
      activeTab: 'dimensions',
    };

    ///This should be a table
    this.dimensionsQueryManager = new QueryManager({
      processQuery: async () => {
        const { datasourceId } = this.props;
        const resp = await queryDruidSql<ColumnMetadata>({
          query: `SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '${datasourceId}'`,
        });
        const dimensionArray = resp.map(object => object.COLUMN_NAME);
        return JSON.stringify(dimensionArray, undefined, 2);
      },
      onStateChange: ({ result, error }) => {
        this.setState({ dimensions: result, error });
      },
    });
  }

  componentDidMount(): void {
    this.dimensionsQueryManager.runQuery(null);
  }

  render(): React.ReactNode {
    const { onClose, datasourceId, actions } = this.props;
    const { activeTab, dimensions, error } = this.state;

    const taskTableSideButtonMetadata: SideButtonMetaData[] = [
      {
        icon: 'list-columns',
        text: 'Dimensions',
        active: activeTab === 'dimensions',
        onClick: () => this.setState({ activeTab: 'dimensions' }),
      },
    ];

    return (
      <TableActionDialog
        isOpen
        sideButtonMetadata={taskTableSideButtonMetadata}
        onClose={onClose}
        title={`Datasource: ${datasourceId}`}
        actions={actions}
      >
        {activeTab === 'dimensions' && (
          <ShowValue
            jsonValue={dimensions ? dimensions : error}
            downloadFilename={`datasource-dimensions-${datasourceId}.json`}
          />
        )}
      </TableActionDialog>
    );
  }
}
