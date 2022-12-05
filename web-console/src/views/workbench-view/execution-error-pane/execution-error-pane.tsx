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

import { Callout } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { ClickToCopy, ExternalLink } from '../../../components';
import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import { Execution } from '../../../druid-models';
import { getLink } from '../../../links';
import { downloadQueryDetailArchive } from '../../../utils';

import './execution-error-pane.scss';

export interface ExecutionErrorPaneProps {
  execution: Execution;
}

export const ExecutionErrorPane = React.memo(function ExecutionErrorPane(
  props: ExecutionErrorPaneProps,
) {
  const { execution } = props;
  const [stackToShow, setStackToShow] = useState<string | undefined>();
  if (!execution.error) return null;

  const { error, exceptionStackTrace, taskId, host } = execution.error;

  return (
    <Callout className="execution-error-pane" icon={IconNames.ERROR}>
      <p className="error-message-text">
        {error.errorCode && (
          <>
            <ExternalLink href={getLink('DOCS_MSQ_ERROR')}>{error.errorCode}</ExternalLink>
            {': '}
          </>
        )}
        {error.errorMessage || (exceptionStackTrace || '').split('\n')[0]}
        {exceptionStackTrace && (
          <>
            {' '}
            <a
              onClick={() => {
                setStackToShow(exceptionStackTrace);
              }}
            >
              (Stack trace)
            </a>
          </>
        )}
      </p>
      {taskId && (
        <div>
          Failed task ID: <ClickToCopy text={taskId} />
          {host && (
            <>
              {' (on host: '}
              <ClickToCopy text={host} />)
            </>
          )}
        </div>
      )}
      <div>
        {'Debug: '}
        <a
          onClick={() => {
            void downloadQueryDetailArchive(execution.id);
          }}
        >
          get query detail archive
        </a>
      </div>

      {stackToShow && (
        <ShowValueDialog
          size="large"
          title="Full stack trace"
          onClose={() => setStackToShow(undefined)}
          str={stackToShow}
        />
      )}
    </Callout>
  );
});
