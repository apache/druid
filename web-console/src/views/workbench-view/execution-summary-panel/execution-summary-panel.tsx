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

import { Button, ButtonGroup, Menu, MenuDivider, MenuItem, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React from 'react';

import { Execution } from '../../../druid-models';
import { downloadQueryResults, formatDurationHybrid, pluralIfNeeded } from '../../../utils';

import './execution-summary-panel.scss';

export interface ExecutionSummaryPanelProps {
  execution: Execution | undefined;
  onExecutionDetail(): void;
  onReset?: () => void;
}

export const ExecutionSummaryPanel = React.memo(function ExecutionSummaryPanel(
  props: ExecutionSummaryPanelProps,
) {
  const { execution, onExecutionDetail, onReset } = props;
  const queryResult = execution?.result;

  const buttons: JSX.Element[] = [];

  if (queryResult) {
    const wrapQueryLimit = queryResult.getSqlOuterLimit();
    const hasMoreResults = queryResult.getNumResults() === wrapQueryLimit;

    const resultCount = hasMoreResults
      ? `${queryResult.getNumResults() - 1}+ results`
      : pluralIfNeeded(queryResult.getNumResults(), 'result');

    const warningCount = execution?.stages?.getWarningCount();

    const handleDownload = (format: string) => {
      downloadQueryResults(queryResult, `results-${execution.id}.${format}`, format);
    };

    buttons.push(
      <Button
        key="results"
        minimal
        text={
          resultCount +
          (warningCount ? ` and ${pluralIfNeeded(warningCount, 'warning')}` : '') +
          (execution.duration ? ` in ${formatDurationHybrid(execution.duration)}` : '')
        }
        onClick={() => {
          if (!execution) return;
          if (execution.engine === 'sql-msq-task') {
            onExecutionDetail();
          }
        }}
      />,
      <Popover2
        key="download"
        className="download-button"
        content={
          <Menu>
            <MenuDivider title="Download results as..." />
            <MenuItem text="CSV" onClick={() => handleDownload('csv')} />
            <MenuItem text="TSV" onClick={() => handleDownload('tsv')} />
            <MenuItem text="JSON (new line delimited)" onClick={() => handleDownload('json')} />
          </Menu>
        }
        position={Position.BOTTOM_RIGHT}
      >
        <Button icon={IconNames.DOWNLOAD} minimal />
      </Popover2>,
    );
  }

  if (onReset) {
    buttons.push(<Button key="reset" icon={IconNames.CROSS} minimal onClick={onReset} />);
  }

  return (
    <ButtonGroup className="execution-summary-panel" minimal>
      {buttons}
    </ButtonGroup>
  );
});
