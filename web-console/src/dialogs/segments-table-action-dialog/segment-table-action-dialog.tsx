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

import { ShowJson } from '../../components';
import { Api } from '../../singletons';
import { BasicAction } from '../../utils/basic-action';
import { SideButtonMetaData, TableActionDialog } from '../table-action-dialog/table-action-dialog';

import { SegmentsPreviewPane } from './segments-preview-pane/segments-preview-pane';

interface SegmentTableActionDialogProps {
  segmentId: string;
  datasourceId: string;
  actions: BasicAction[];
  onClose: () => void;
}

export const SegmentTableActionDialog = React.memo(function SegmentTableActionDialog(
  props: SegmentTableActionDialogProps,
) {
  const { segmentId, onClose, datasourceId, actions } = props;
  const [activeTab, setActiveTab] = useState('metadata');

  const taskTableSideButtonMetadata: SideButtonMetaData[] = [
    {
      icon: IconNames.MANUALLY_ENTERED_DATA,
      text: 'Metadata',
      active: activeTab === 'metadata',
      onClick: () => setActiveTab('metadata'),
    },
    {
      icon: IconNames.TH,
      text: 'Records',
      active: activeTab === 'records',
      onClick: () => setActiveTab('records'),
    },
  ];

  return (
    <TableActionDialog
      sideButtonMetadata={taskTableSideButtonMetadata}
      onClose={onClose}
      title={`Segment: ${segmentId}`}
      actions={actions}
    >
      {activeTab === 'metadata' && (
        <ShowJson
          endpoint={`/druid/coordinator/v1/metadata/datasources/${Api.encodePath(
            datasourceId,
          )}/segments/${Api.encodePath(segmentId)}`}
          downloadFilename={`Segment-metadata-${segmentId}.json`}
        />
      )}
      {activeTab === 'records' && <SegmentsPreviewPane segmentId={segmentId} />}
    </TableActionDialog>
  );
});
