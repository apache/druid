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
  Intent,
  Menu,
  MenuDivider,
  MenuItem,
  Popover,
  Position,
  Tooltip,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import copy from 'copy-to-clipboard';
import { QueryResult } from 'druid-query-toolkit';
import React from 'react';

import { AppToaster } from '../../../singletons';
import { pluralIfNeeded } from '../../../utils';

import './query-extra-info.scss';

export interface QueryExtraInfoProps {
  queryResult: QueryResult;
  onDownload: (filename: string, format: string) => void;
}

export const QueryExtraInfo = React.memo(function QueryExtraInfo(props: QueryExtraInfoProps) {
  const { queryResult, onDownload } = props;

  function handleQueryInfoClick() {
    const id = queryResult.queryId || queryResult.sqlQueryId;
    if (!id) return;

    copy(id, { format: 'text/plain' });
    AppToaster.show({
      message: 'Query ID copied to clipboard',
      intent: Intent.SUCCESS,
    });
  }

  function handleDownload(format: string) {
    const id = queryResult.queryId || queryResult.sqlQueryId;
    if (!id) return;

    onDownload(`query-${id}.${format}`, format);
  }

  const downloadMenu = (
    <Menu className="download-format-menu">
      <MenuDivider title="Download as:" />
      <MenuItem text="CSV" onClick={() => handleDownload('csv')} />
      <MenuItem text="TSV" onClick={() => handleDownload('tsv')} />
      <MenuItem text="JSON (new line delimited)" onClick={() => handleDownload('json')} />
    </Menu>
  );

  const wrapQueryLimit = queryResult.getSqlOuterLimit();
  let resultCount: string;
  if (wrapQueryLimit && queryResult.getNumResults() === wrapQueryLimit) {
    resultCount = `${queryResult.getNumResults() - 1}+ results`;
  } else {
    resultCount = pluralIfNeeded(queryResult.getNumResults(), 'result');
  }

  let tooltipContent: JSX.Element | undefined;
  if (queryResult.queryId) {
    tooltipContent = (
      <>
        Query ID: <strong>{queryResult.queryId}</strong> (click to copy)
      </>
    );
  } else if (queryResult.sqlQueryId) {
    tooltipContent = (
      <>
        SQL query ID: <strong>{queryResult.sqlQueryId}</strong> (click to copy)
      </>
    );
  }

  return (
    <div className="query-extra-info">
      {typeof queryResult.queryDuration !== 'undefined' && (
        <div className="query-info" onClick={handleQueryInfoClick}>
          <Tooltip content={tooltipContent} hoverOpenDelay={500}>
            {`${resultCount} in ${(queryResult.queryDuration / 1000).toFixed(2)}s`}
          </Tooltip>
        </div>
      )}
      <Popover className="download-button" content={downloadMenu} position={Position.BOTTOM_RIGHT}>
        <Button icon={IconNames.DOWNLOAD} minimal />
      </Popover>
    </div>
  );
});
