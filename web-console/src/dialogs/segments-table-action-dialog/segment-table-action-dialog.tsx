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

import { ShowJson } from '../../components';
import { BasicAction, basicActionsToButtons } from '../../utils/basic-action';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

interface SegmentTableActionDialogProps extends IDialogProps {
  segmentId?: string;
  dataSourceId?: string;
  actions: BasicAction[];
  onClose: () => void;
}

interface SegmentTableActionDialogState {
  activeTab: 'metadata';
}

export class SegmentTableActionDialog extends React.PureComponent<
  SegmentTableActionDialogProps,
  SegmentTableActionDialogState
> {
  constructor(props: SegmentTableActionDialogProps) {
    super(props);
    this.state = {
      activeTab: 'metadata',
    };
  }

  render(): React.ReactNode {
    const { segmentId, onClose, dataSourceId, actions } = this.props;
    const { activeTab } = this.state;

    const taskTableSideButtonMetadata: SideButtonMetaData[] = [
      {
        icon: 'manually-entered-data',
        text: 'Metadata',
        active: activeTab === 'metadata',
        onClick: () => this.setState({ activeTab: 'metadata' }),
      },
    ];

    return (
      <TableActionDialog
        isOpen
        sideButtonMetadata={taskTableSideButtonMetadata}
        onClose={onClose}
        title={`Segment: ${segmentId}`}
        bottomButtons={basicActionsToButtons(actions)}
      >
        {activeTab === 'metadata' && (
          <ShowJson
            endpoint={`/druid/coordinator/v1/metadata/datasources/${dataSourceId}/segments/${segmentId}`}
            downloadFilename={`Segment-metadata-${segmentId}.json`}
          />
        )}
      </TableActionDialog>
    );
  }
}
