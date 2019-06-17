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

import { IDialogProps, TextArea } from '@blueprintjs/core';
import React from 'react';

import { BasicAction, basicActionsToButtons } from '../../utils/basic-action';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

interface SegmentTableActionDialogProps extends IDialogProps {
  taskId: string | null;
  dimensions: string[];
  metrics: string[];
  actions: BasicAction[];
  onClose: () => void;
}

interface SegmentTableActionDialogState {
  activeTab: 'dimensions' | 'metrics';
}

export class SegmentTableActionDialog extends React.PureComponent<SegmentTableActionDialogProps, SegmentTableActionDialogState> {
  constructor(props: SegmentTableActionDialogProps) {
    super(props);
    this.state = {
      activeTab: 'dimensions'
    };
  }

  render(): React.ReactNode {
    const { taskId, actions, onClose, dimensions, metrics } = this.props;
    const { activeTab } = this.state;

    const taskTableSideButtonMetadata: SideButtonMetaData[] = [
      {
        icon: 'split-columns',
        text: 'Dimensions (' + dimensions.length + ')',
        active: activeTab === 'dimensions',
        onClick: () => this.setState({ activeTab: 'dimensions' })
      },
      {
        icon: 'series-configuration',
        text: 'Metrics (' + metrics.length + ')',
        active: activeTab === 'metrics',
        onClick: () => this.setState({ activeTab: 'metrics' })
      }
    ];

    return <TableActionDialog
        isOpen
        sideButtonMetadata={taskTableSideButtonMetadata}
        onClose={onClose}
        title={`Segment: ${taskId}`}
        bottomButtons={basicActionsToButtons(actions)}
    >
      { activeTab === 'dimensions' &&
        <div className={'show-json'}>
        <div className="main-area">
          <TextArea
            readOnly
            value={dimensions.length ? dimensions.join().split(',').join('\n') : 'No dimensions'}
          />
        </div>
        </div>
      }
      { activeTab === 'metrics' &&
        <div className={'show-json'}>
        <div className="main-area">
          <TextArea
            readOnly
            value={metrics.length ? metrics.join().split(',').join('\n') : 'No metrics'}
          />
        </div>
        </div>
      }
    </TableActionDialog>;
  }
}
