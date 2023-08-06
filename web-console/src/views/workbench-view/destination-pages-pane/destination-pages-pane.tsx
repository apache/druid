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

import { AnchorButton, Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';
import ReactTable from 'react-table';

import type { Execution } from '../../../druid-models';
import { Api, UrlBaser } from '../../../singletons';
import {
  clamp,
  downloadUrl,
  formatBytes,
  formatInteger,
  pluralIfNeeded,
  wait,
} from '../../../utils';

const MAX_DETAIL_ROWS = 20;

interface DestinationPagesPaneProps {
  execution: Execution;
}

export const DestinationPagesPane = React.memo(function DestinationPagesPane(
  props: DestinationPagesPaneProps,
) {
  const { execution } = props;
  const destination = execution.destination;
  const pages = execution.destinationPages;
  if (!pages) return null;
  const id = Api.encodePath(execution.id);

  const numTotalRows = destination?.numTotalRows;

  function getPageUrl(pageIndex: number) {
    return UrlBaser.base(`/druid/v2/sql/statements/${id}/results?page=${pageIndex}`);
  }

  function getPageFilename(pageIndex: number) {
    return `${id}_page${pageIndex}.jsonl`;
  }

  async function downloadAllPages() {
    if (!pages) return;
    for (let i = 0; i < pages.length; i++) {
      downloadUrl(getPageUrl(i), getPageFilename(i));
      await wait(100);
    }
  }

  return (
    <div className="execution-details-pane">
      <p>
        {`${
          typeof numTotalRows === 'number' ? pluralIfNeeded(numTotalRows, 'row') : 'Results'
        } have been written to ${pluralIfNeeded(pages.length, 'page')}. `}
        {pages.length > 1 && (
          <Button
            icon={IconNames.DOWNLOAD}
            text={`Download all data (as ${pluralIfNeeded(pages.length, 'file')})`}
            minimal
            onClick={() => void downloadAllPages()}
          />
        )}
      </p>
      <ReactTable
        className="padded-header"
        data={pages}
        loading={false}
        sortable
        defaultSorted={[{ id: 'id', desc: false }]}
        defaultPageSize={clamp(pages.length, 1, MAX_DETAIL_ROWS)}
        showPagination={pages.length > MAX_DETAIL_ROWS}
        columns={[
          {
            Header: 'Page number',
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
            sortable: false,
            width: 300,
            Cell: ({ value }) => (
              <AnchorButton
                icon={IconNames.DOWNLOAD}
                text="download .jsonl"
                minimal
                href={getPageUrl(value)}
                download={getPageFilename(value)}
              />
            ),
          },
        ]}
      />
    </div>
  );
});
