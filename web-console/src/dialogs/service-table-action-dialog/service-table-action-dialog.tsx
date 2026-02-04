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

import React, { useState } from 'react';

import type { BasicAction } from '../../utils/basic-action';
import type { SideButtonMetaData } from '../table-action-dialog/table-action-dialog';
import { TableActionDialog } from '../table-action-dialog/table-action-dialog';

import { ServicePropertiesTable } from './service-properties-table/service-properties-table';

type ServiceTableActionDialogTab = 'properties';

interface ServiceTableActionDialogProps {
  service: string;
  actions: BasicAction[];
  onClose: () => void;
}

export const ServiceTableActionDialog = React.memo(function ServiceTableActionDialog(
  props: ServiceTableActionDialogProps,
) {
  const { service, actions, onClose } = props;
  const [activeTab, setActiveTab] = useState<ServiceTableActionDialogTab>('properties');

  const sideButtonMetadata: SideButtonMetaData[] = [
    {
      icon: 'properties',
      text: 'Properties',
      active: activeTab === 'properties',
      onClick: () => setActiveTab('properties'),
    },
  ];

  return (
    <TableActionDialog
      sideButtonMetadata={sideButtonMetadata}
      onClose={onClose}
      title={`Service: ${service}`}
      actions={actions}
    >
      {activeTab === 'properties' && <ServicePropertiesTable server={service} />}
    </TableActionDialog>
  );
});
