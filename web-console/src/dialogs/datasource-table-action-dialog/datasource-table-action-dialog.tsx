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

import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { BasicAction } from '../../utils/basic-action';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

import { DatasourceColumnsTable } from './datasource-columns-table/datasource-columns-table';
import { DatasourcePreviewPane } from './datasource-preview-pane/datasource-preview-pane';

interface DatasourceTableActionDialogProps {
  datasource: string;
  actions: BasicAction[];
  onClose(): void;
}

export const DatasourceTableActionDialog = React.memo(function DatasourceTableActionDialog(
  props: DatasourceTableActionDialogProps,
) {
  const { datasource, actions, onClose } = props;
  const [activeTab, setActiveTab] = useState<'records' | 'columns'>('records');

  const sideButtonMetadata: SideButtonMetaData[] = [
    {
      icon: IconNames.TH,
      text: 'Records',
      active: activeTab === 'records',
      onClick: () => setActiveTab('records'),
    },
    {
      icon: IconNames.LIST_COLUMNS,
      text: 'Columns',
      active: activeTab === 'columns',
      onClick: () => setActiveTab('columns'),
    },
  ];

  return (
    <TableActionDialog
      sideButtonMetadata={sideButtonMetadata}
      onClose={onClose}
      title={`Datasource: ${datasource}`}
      actions={actions}
    >
      {activeTab === 'records' && <DatasourcePreviewPane datasource={datasource} />}
      {activeTab === 'columns' && <DatasourceColumnsTable datasource={datasource} />}
    </TableActionDialog>
  );
});
