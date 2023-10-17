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
import { T } from '@druid-toolkit/query';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { FancyTabPane } from '../../../components';
import type { Execution } from '../../../druid-models';
import { formatDuration, formatDurationWithMs, pluralIfNeeded } from '../../../utils';
import { DestinationPagesPane } from '../destination-pages-pane/destination-pages-pane';
import { ExecutionErrorPane } from '../execution-error-pane/execution-error-pane';
import { ExecutionStagesPane } from '../execution-stages-pane/execution-stages-pane';
import { ExecutionWarningsPane } from '../execution-warnings-pane/execution-warnings-pane';
import { FlexibleQueryInput } from '../flexible-query-input/flexible-query-input';
import { ResultTablePane } from '../result-table-pane/result-table-pane';

import './execution-details-pane.scss';

export type ExecutionDetailsTab =
  | 'general'
  | 'sql'
  | 'native'
  | 'result'
  | 'pages'
  | 'error'
  | 'warnings'
  | 'segmentStatus';

interface ExecutionDetailsPaneProps {
  execution: Execution;
  initTab?: ExecutionDetailsTab;
  goToTask(taskId: string): void;
}

export const ExecutionDetailsPane = React.memo(function ExecutionDetailsPane(
  props: ExecutionDetailsPaneProps,
) {
  const { execution, initTab, goToTask } = props;
  const [activeTab, setActiveTab] = useState<ExecutionDetailsTab>(initTab || 'general');
  const segmentStatusDescription = execution.getSegmentStatusDescription();

  function renderContent() {
    switch (activeTab) {
      case 'general': {
        const ingestDatasource = execution.getIngestDatasource();
        return (
          <div>
            <p>{`General info for ${execution.id}${
              ingestDatasource ? ` ingesting into ${T(ingestDatasource)}` : ''
            }`}</p>
            {execution.destination && (
              <p>
                {`Results written to ${execution.destination.type}`}
                {execution.destinationPages
                  ? ` (${pluralIfNeeded(execution.destinationPages.length, 'page')})`
                  : undefined}
              </p>
            )}
            {execution.error && <ExecutionErrorPane execution={execution} />}
            {execution.stages ? (
              <ExecutionStagesPane
                execution={execution}
                onErrorClick={() => setActiveTab('error')}
                onWarningClick={() => setActiveTab('warnings')}
                goToTask={goToTask}
              />
            ) : (
              <p>No stage info was reported.</p>
            )}
          </div>
        );
      }

      case 'sql':
      case 'native':
        return (
          <FlexibleQueryInput
            queryString={
              activeTab === 'sql'
                ? String(execution.sqlQuery)
                : JSONBig.stringify(execution.nativeQuery, undefined, 2)
            }
            leaveBackground
          />
        );

      case 'result':
        if (!execution.result) return;
        return (
          <ResultTablePane
            runeMode={execution.engine === 'native'}
            queryResult={execution.result}
            onExport={() => {}}
            onQueryAction={() => {}}
          />
        );

      case 'pages':
        if (!execution.destinationPages) return;
        return <DestinationPagesPane execution={execution} />;

      case 'error':
        return <ExecutionErrorPane execution={execution} />;

      case 'warnings':
        return <ExecutionWarningsPane execution={execution} />;

      case 'segmentStatus':
        return (
          <>
            <p>
              Duration:{' '}
              {segmentStatusDescription.duration
                ? formatDurationWithMs(segmentStatusDescription.duration)
                : '-'}
              {execution.duration
                ? ` (query duration was ${formatDuration(execution.duration)})`
                : ''}
            </p>
            <p>Total segments: {segmentStatusDescription.totalSegments ?? '-'}</p>
            <p>Used segments: {segmentStatusDescription.usedSegments ?? '-'}</p>
            <p>Precached segments: {segmentStatusDescription.precachedSegments ?? '-'}</p>
            <p>On demand segments: {segmentStatusDescription.onDemandSegments ?? '-'}</p>
          </>
        );

      default:
        return;
    }
  }

  return (
    <FancyTabPane
      className="execution-details-pane"
      activeTab={activeTab}
      onActivateTab={setActiveTab as any}
      tabs={[
        {
          id: 'general',
          label: 'General',
          icon: IconNames.MANY_TO_ONE,
        },
        Boolean(execution.sqlQuery) && {
          id: 'sql',
          label: 'SQL query',
          icon: IconNames.APPLICATION,
        },
        Boolean(execution.nativeQuery) && {
          id: 'native',
          label: 'Native query',
          icon: IconNames.COG,
        },
        Boolean(execution.segmentStatus) && {
          id: 'segmentStatus',
          label: 'Segments',
          icon: IconNames.HEAT_GRID,
        },
        execution.result && {
          id: 'result',
          label: 'Results',
          icon: IconNames.TH,
        },
        execution.destinationPages && {
          id: 'pages',
          label: 'Result pages',
          icon: IconNames.APPLICATIONS,
        },
        execution.error && {
          id: 'error',
          label: 'Error',
          icon: IconNames.ERROR,
        },
        execution.warnings && {
          id: 'warnings',
          label: 'Warnings',
          icon: IconNames.WARNING_SIGN,
        },
      ]}
    >
      {renderContent()}
    </FancyTabPane>
  );
});
