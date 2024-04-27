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

import { AnchorButton, Button, Intent, Menu, MenuItem, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';
import ReactTable from 'react-table';

import type { Execution } from '../../../druid-models';
import { SMALL_TABLE_PAGE_SIZE } from '../../../react-table';
import { Api, UrlBaser } from '../../../singletons';
import {
  clamp,
  downloadUrl,
  formatBytes,
  formatInteger,
  pluralIfNeeded,
  tickIcon,
  wait,
} from '../../../utils';

import './destination-pages-pane.scss';

type ResultFormat = 'object' | 'array' | 'objectLines' | 'arrayLines' | 'csv';

const RESULT_FORMATS: ResultFormat[] = ['objectLines', 'object', 'arrayLines', 'array', 'csv'];

function resultFormatToExtension(resultFormat: ResultFormat): string {
  switch (resultFormat) {
    case 'object':
    case 'array':
      return 'json';

    case 'objectLines':
    case 'arrayLines':
      return 'jsonl';

    case 'csv':
      return 'csv';
  }
}

const RESULT_FORMAT_LABEL: Record<ResultFormat, string> = {
  object: 'Array of objects',
  array: 'Array of arrays',
  objectLines: 'JSON Lines',
  arrayLines: 'JSON Lines but every row is an array',
  csv: 'CSV',
};

interface DestinationPagesPaneProps {
  execution: Execution;
}

export const DestinationPagesPane = React.memo(function DestinationPagesPane(
  props: DestinationPagesPaneProps,
) {
  const { execution } = props;
  const [desiredResultFormat, setDesiredResultFormat] = useState<ResultFormat>('objectLines');
  const desiredExtension = resultFormatToExtension(desiredResultFormat);

  const destination = execution.destination;
  const pages = execution.destinationPages;
  if (!pages) return null;
  const id = Api.encodePath(execution.id);

  const numTotalRows = destination?.numTotalRows;

  function getPageUrl(pageIndex: number) {
    return UrlBaser.base(
      `/druid/v2/sql/statements/${id}/results?page=${pageIndex}&resultFormat=${desiredResultFormat}`,
    );
  }

  function getPageFilename(pageIndex: number, numPages: number) {
    const numPagesString = String(numPages);
    const pageNumberString = String(pageIndex + 1).padStart(numPagesString.length, '0');
    return `${id}_page_${pageNumberString}_of_${numPagesString}.${desiredExtension}`;
  }

  async function downloadAllPages() {
    if (!pages) return;
    const numPages = pages.length;
    for (let i = 0; i < pages.length; i++) {
      downloadUrl(getPageUrl(i), getPageFilename(i, numPages));
      await wait(100);
    }
  }

  const numPages = pages.length;
  return (
    <div className="destination-pages-pane">
      <p>
        {`${
          typeof numTotalRows === 'number' ? pluralIfNeeded(numTotalRows, 'row') : 'Results'
        } have been written to ${pluralIfNeeded(numPages, 'page')}. `}
      </p>
      <p>
        Format when downloading:{' '}
        <Popover2
          minimal
          position={Position.BOTTOM_LEFT}
          content={
            <Menu>
              {RESULT_FORMATS.map((resultFormat, i) => (
                <MenuItem
                  key={i}
                  icon={tickIcon(desiredResultFormat === resultFormat)}
                  text={RESULT_FORMAT_LABEL[resultFormat]}
                  label={resultFormat}
                  onClick={() => setDesiredResultFormat(resultFormat)}
                />
              ))}
            </Menu>
          }
        >
          <Button
            text={RESULT_FORMAT_LABEL[desiredResultFormat]}
            rightIcon={IconNames.CARET_DOWN}
          />
        </Popover2>{' '}
        {pages.length > 1 && (
          <Button
            intent={Intent.PRIMARY}
            icon={IconNames.DOWNLOAD}
            text={`Download all data (${pluralIfNeeded(numPages, 'file')})`}
            onClick={() => void downloadAllPages()}
          />
        )}
      </p>
      <ReactTable
        data={pages}
        loading={false}
        sortable={false}
        defaultPageSize={clamp(numPages, 1, SMALL_TABLE_PAGE_SIZE)}
        showPagination={numPages > SMALL_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: 'Page ID',
            id: 'id',
            accessor: 'id',
            className: 'padded',
            width: 100,
          },
          {
            Header: 'Number of rows',
            id: 'numRows',
            accessor: 'numRows',
            className: 'padded',
            width: 200,
            Cell: ({ value }) => formatInteger(value),
          },
          {
            Header: 'Size',
            id: 'sizeInBytes',
            accessor: 'sizeInBytes',
            className: 'padded',
            width: 200,
            Cell: ({ value }) => formatBytes(value),
          },
          {
            Header: '',
            id: 'download',
            accessor: 'id',
            width: 300,
            Cell: ({ value }) => (
              <AnchorButton
                className="download-button"
                icon={IconNames.DOWNLOAD}
                text="Download"
                minimal
                href={getPageUrl(value)}
                download={getPageFilename(value, numPages)}
              />
            ),
          },
        ]}
      />
    </div>
  );
});
