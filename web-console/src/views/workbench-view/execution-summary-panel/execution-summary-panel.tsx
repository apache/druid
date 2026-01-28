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

import {
  Button,
  ButtonGroup,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { JSX } from 'react';
import React, { useState } from 'react';

import type { Execution } from '../../../druid-models';
import type { FileFormat } from '../../../utils';
import {
  copyAndAlert,
  copyQueryResultsToClipboard,
  downloadQueryResults,
  FILE_FORMAT_TO_LABEL,
  FILE_FORMATS,
  formatDurationHybrid,
  formatInteger,
  oneOf,
  pluralIfNeeded,
} from '../../../utils';
import { DestinationPagesDialog } from '../destination-pages-dialog/destination-pages-dialog';

import './execution-summary-panel.scss';

export interface ExecutionSummaryPanelProps {
  execution: Execution | undefined;
  queryErrorDuration: number | undefined;
  onExecutionDetail(): void;
  onReset?: () => void;
}

export const ExecutionSummaryPanel = React.memo(function ExecutionSummaryPanel(
  props: ExecutionSummaryPanelProps,
) {
  const { execution, queryErrorDuration, onExecutionDetail, onReset } = props;
  const [showDestinationPages, setShowDestinationPages] = useState(false);
  const queryResult = execution?.result;

  const buttons: JSX.Element[] = [];

  if (typeof queryErrorDuration === 'number') {
    buttons.push(
      <Button
        key="timing"
        minimal
        text={`Error after ${formatDurationHybrid(queryErrorDuration)}`}
      />,
    );
  }

  if (queryResult) {
    const wrapQueryLimit = queryResult.getSqlOuterLimit();
    let resultCount: string;
    const numTotalRows = execution?.destination?.numTotalRows;
    if (typeof wrapQueryLimit === 'undefined' && typeof numTotalRows === 'number') {
      resultCount = pluralIfNeeded(numTotalRows, 'result');
    } else {
      const hasMoreResults = queryResult.getNumResults() === wrapQueryLimit;

      resultCount = hasMoreResults
        ? `${formatInteger(queryResult.getNumResults() - 1)}+ results`
        : pluralIfNeeded(queryResult.getNumResults(), 'result');
    }

    const warningCount = execution?.stages?.getWarningCount();

    const handleDownload = (format: FileFormat) => {
      downloadQueryResults(queryResult, `results-${execution.id}.${format}`, format);
    };

    const handleCopy = (format: FileFormat) => {
      copyQueryResultsToClipboard(queryResult, format);
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
          if (oneOf(execution.engine, 'sql-msq-task', 'sql-msq-dart')) {
            onExecutionDetail();
          } else {
            copyAndAlert(execution.id, `Query ID (${execution.id}) copied to clipboard`);
          }
        }}
        data-tooltip={
          execution &&
          (oneOf(execution.engine, 'sql-msq-task', 'sql-msq-dart')
            ? `Open details for\n${execution.id}`
            : `Query ID\n${execution.id}\n(click to copy)`)
        }
      />,
      <Popover
        key="download"
        className="download-button"
        position={Position.BOTTOM_RIGHT}
        content={
          <Menu>
            {execution.destinationPages && (
              <>
                <MenuDivider title="Download in bulk" />
                <MenuItem text="Show result pages" onClick={() => setShowDestinationPages(true)} />
                <MenuDivider title="Download data in view" />
              </>
            )}
            <MenuItem text="Download results as...">
              {FILE_FORMATS.map(fileFormat => (
                <MenuItem
                  key={fileFormat}
                  text={FILE_FORMAT_TO_LABEL[fileFormat]}
                  onClick={() => handleDownload(fileFormat)}
                />
              ))}
            </MenuItem>
            <MenuItem text="Copy to clipboard as...">
              {FILE_FORMATS.map(fileFormat => (
                <MenuItem
                  key={fileFormat}
                  text={FILE_FORMAT_TO_LABEL[fileFormat]}
                  onClick={() => handleCopy(fileFormat)}
                />
              ))}
            </MenuItem>
          </Menu>
        }
      >
        <Button icon={IconNames.DOWNLOAD} data-tooltip="Download" minimal />
      </Popover>,
    );
  }

  if (onReset) {
    buttons.push(
      <Button
        key="reset"
        icon={IconNames.CROSS}
        data-tooltip="Clear output"
        minimal
        onClick={onReset}
      />,
    );
  }

  return (
    <ButtonGroup className="execution-summary-panel" minimal>
      {buttons}
      {showDestinationPages && execution && (
        <DestinationPagesDialog
          execution={execution}
          onClose={() => setShowDestinationPages(false)}
        />
      )}
    </ButtonGroup>
  );
});
