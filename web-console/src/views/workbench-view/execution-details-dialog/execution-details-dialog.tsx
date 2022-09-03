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

import { Button, Classes, Dialog } from '@blueprintjs/core';
import React from 'react';

import { Execution } from '../../../druid-models';
import { ExecutionDetailsTab } from '../execution-details-pane/execution-details-pane';
import { ExecutionDetailsPaneLoader } from '../execution-details-pane-loader/execution-details-pane-loader';

import './execution-details-dialog.scss';

export interface ExecutionDetailsDialogProps {
  id: string;
  initTab?: ExecutionDetailsTab;
  initExecution?: Execution;
  goToIngestion(taskId: string): void;
  onClose(): void;
}

export const ExecutionDetailsDialog = React.memo(function ExecutionDetailsDialog(
  props: ExecutionDetailsDialogProps,
) {
  const { id, initTab, initExecution, goToIngestion, onClose } = props;

  return (
    <Dialog className="execution-details-dialog" isOpen onClose={onClose} title="Execution details">
      <div className={Classes.DIALOG_BODY}>
        <ExecutionDetailsPaneLoader
          id={id}
          initTab={initTab}
          initExecution={initExecution}
          goToIngestion={goToIngestion}
        />
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
});
