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

import { SqlTableRef } from 'druid-query-toolkit';
import React from 'react';

import { Execution, WorkbenchQuery } from '../../../druid-models';
import { formatDuration, pluralIfNeeded } from '../../../utils';
import { ExecutionDetailsTab } from '../execution-details-pane/execution-details-pane';

import './ingest-success-pane.scss';

export interface IngestSuccessPaneProps {
  execution: Execution;
  onDetails(id: string, initTab?: ExecutionDetailsTab): void;
  onQueryTab?(newQuery: WorkbenchQuery, tabName?: string): void;
}

export const IngestSuccessPane = React.memo(function IngestSuccessPane(
  props: IngestSuccessPaneProps,
) {
  const { execution, onDetails, onQueryTab } = props;

  const datasource = execution.getIngestDatasource();
  if (!datasource) return null;

  const { stages } = execution;
  const lastStage = stages?.getLastStage();

  const rows =
    stages && lastStage && lastStage.definition.processor.type === 'segmentGenerator'
      ? stages.getTotalCounterForStage(lastStage, 'input0', 'rows') // Assume input0 since we know the segmentGenerator will only ever have one stage input
      : -1;

  const table = SqlTableRef.create(datasource);

  const warnings = stages?.getWarningCount() || 0;

  const duration = execution.duration;
  return (
    <div className="ingest-success-pane">
      <p>
        {`${rows < 0 ? 'Data' : pluralIfNeeded(rows, 'row')} inserted into '${datasource}'.`}
        {warnings > 0 && (
          <>
            {' '}
            <span className="action" onClick={() => onDetails(execution.id, 'warnings')}>
              {pluralIfNeeded(warnings, 'warning')}
            </span>{' '}
            recorded.
          </>
        )}
      </p>
      <p>
        {duration ? `Insert query took ${formatDuration(duration)}. ` : `Insert query completed. `}
        <span className="action" onClick={() => onDetails(execution.id)}>
          Show details
        </span>
      </p>
      {onQueryTab && (
        <p>
          Open new tab with:{' '}
          <span
            className="action"
            onClick={() =>
              onQueryTab(
                WorkbenchQuery.blank().changeQueryString(`SELECT * FROM ${table}`),
                datasource,
              )
            }
          >{`SELECT * FROM ${table}`}</span>
        </p>
      )}
    </div>
  );
});
