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

import { Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import { SqlTableRef } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { Execution, QueryWithContext } from '../../../druid-models';
import { executionBackgroundStatusCheck, reattachTaskExecution } from '../../../helpers';
import { useQueryManager } from '../../../hooks';
import { ExecutionProgressBarPane } from '../../workbench-view/execution-progress-bar-pane/execution-progress-bar-pane';
import { ExecutionStagesPane } from '../../workbench-view/execution-stages-pane/execution-stages-pane';

import './ingestion-progress-dialog.scss';

interface IngestionProgressDialogProps {
  taskId: string;
  goToQuery(queryWithContext: QueryWithContext): void;
  goToIngestion(taskId: string): void;
  onReset(): void;
  onClose(): void;
}

export const IngestionProgressDialog = React.memo(function IngestionProgressDialog(
  props: IngestionProgressDialogProps,
) {
  const { taskId, goToQuery, goToIngestion, onReset, onClose } = props;
  const [showLiveReports, setShowLiveReports] = useState(false);

  const [insertResultState, ingestQueryManager] = useQueryManager<string, Execution, Execution>({
    initQuery: taskId,
    processQuery: async (id, cancelToken) => {
      return await reattachTaskExecution({
        id,
        cancelToken,
        preserveOnTermination: true,
      });
    },
    backgroundStatusCheck: executionBackgroundStatusCheck,
  });

  return (
    <Dialog
      className={classNames('ingestion-progress-dialog', insertResultState.state)}
      onClose={insertResultState.isLoading() ? undefined : onClose}
      isOpen
      title="Ingestion progress"
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        {insertResultState.isLoading() && (
          <>
            <p>
              The data is now being loaded. You can track the task from both the Query and the
              Ingestion views.
            </p>
            <ExecutionProgressBarPane
              execution={insertResultState.intermediate}
              onCancel={() => ingestQueryManager.cancelCurrent()}
              onToggleLiveReports={() => setShowLiveReports(!showLiveReports)}
              showLiveReports={showLiveReports}
            />
            {insertResultState.intermediate?.stages && showLiveReports && (
              <ExecutionStagesPane
                execution={insertResultState.intermediate}
                goToIngestion={goToIngestion}
              />
            )}
          </>
        )}
        {insertResultState.isError() && (
          <p>Error ingesting data: {insertResultState.getErrorMessage()}</p>
        )}
        {insertResultState.data && (
          <p>Done loading data into {insertResultState.data.getIngestDatasource()}</p>
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          {insertResultState.isLoading() && (
            <Button
              icon={IconNames.GANTT_CHART}
              text="Go to Ingestion view"
              rightIcon={IconNames.ARROW_TOP_RIGHT}
              onClick={() => {
                if (!insertResultState.intermediate) return;
                goToIngestion(insertResultState.intermediate.id);
              }}
            />
          )}
          {insertResultState.isError() && <Button text="Close" onClick={onClose} />}
          {insertResultState.data && (
            <>
              <Button icon={IconNames.RESET} text="Reset data loader" onClick={onReset} />
              <Button
                icon={IconNames.APPLICATION}
                text={`Query: ${insertResultState.data.getIngestDatasource()}`}
                rightIcon={IconNames.ARROW_TOP_RIGHT}
                intent={Intent.PRIMARY}
                onClick={() => {
                  if (!insertResultState.data) return;
                  goToQuery({
                    queryString: `SELECT * FROM ${SqlTableRef.create(
                      insertResultState.data.getIngestDatasource()!,
                    )}`,
                  });
                }}
              />
            </>
          )}
        </div>
      </div>
    </Dialog>
  );
});
