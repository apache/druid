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
import * as React from 'react';

import { ShowJson } from '../components/show-json';
import { ShowLog } from '../components/show-log';
import { BasicAction, basicActionsToButtons } from '../utils/basic-action';

import { SideButtonMetaData, TableActionDialog } from './table-action-dialog';

interface  LookupTableActionDialogProps extends IDialogProps {
  lookupId: string;
  actions: BasicAction[];
  onClose: () => void;
}


interface LookupTableActionDialogState {
  activeTab: 'complete' | 'values' ;
}

export class LookupTableActionDialog extends React.Component<LookupTableActionDialogProps, LookupTableActionDialogState> {
  constructor(props: LookupTableActionDialogProps) {
    super(props);
    this.state = {
      activeTab: 'complete'
    };
  }

  render(): React.ReactNode {
    const { lookupId, actions, onClose } = this.props;
    const { activeTab } = this.state;

    const lookupTableSideButtonMetadata: SideButtonMetaData[] = [
      {
        icon: 'map',
        text: 'map of values',
        active: activeTab === 'complete',
        onClick: () => this.setState({ activeTab: 'complete' })
      },
      {
        icon: 'list',
        text: 'values',
        active: activeTab === 'values',
        onClick: () => this.setState({ activeTab: 'values' })
      }
    ];

    return <TableActionDialog
      isOpen
      sideButtonMetadata={lookupTableSideButtonMetadata}
      onClose={onClose}
      title={`Lookup: ${lookupId}`}
      bottomButtons={basicActionsToButtons(actions)}
    >
      {activeTab === 'complete' && <ShowJson endpoint={`/druid/v1/lookups/introspect/${lookupId}`} downloadFilename={`lookup-complete-values-${lookupId}.json`}/>}
      {activeTab === 'values' && <ShowJson endpoint={`/druid/v1/lookups/introspect/${lookupId}/values`} downloadFilename={`look-up-values-${lookupId}.json`}/>}
    </TableActionDialog>;
  }
}
