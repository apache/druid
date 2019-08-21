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

import { DatasourceColumnsTable } from '../../components/datasource-columns-table/datasource-columns-table';
import { BasicAction } from '../../utils/basic-action';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

interface DatasourceTableActionDialogProps extends IDialogProps {
  datasourceId?: string;
  actions: BasicAction[];
  onClose: () => void;
}

interface DatasourceTableActionDialogState {
  activeTab: 'columns';
}

export class DatasourceTableActionDialog extends React.PureComponent<
  DatasourceTableActionDialogProps,
  DatasourceTableActionDialogState
> {
  constructor(props: DatasourceTableActionDialogProps) {
    super(props);
    this.state = {
      activeTab: 'columns',
    };
  }

  render(): React.ReactNode {
    const { onClose, datasourceId, actions } = this.props;
    const { activeTab } = this.state;

    const taskTableSideButtonMetadata: SideButtonMetaData[] = [
      {
        icon: 'list-columns',
        text: 'Columns',
        active: activeTab === 'columns',
        onClick: () => this.setState({ activeTab: 'columns' }),
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
        {activeTab === 'columns' && (
          <DatasourceColumnsTable
            datasourceId={datasourceId ? datasourceId : ''}
            downloadFilename={`datasource-dimensions-${datasourceId}.json`}
          />
        )}
      </TableActionDialog>
    );
  }
}
