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
import React from 'react';
import ReactTable, { Filter } from 'react-table';

import { Loader } from '../../components';
import { useQueryManager } from '../../hooks';
import { Api, UrlBaser } from '../../singletons';

import './status-dialog.scss';

export function anywhereMatcher(filter: Filter, row: any): boolean {
  return String(row[filter.id]).includes(filter.value);
}

interface StatusModule {
  artifact: string;
  name: string;
  version: string;
}

interface StatusResponse {
  version: string;
  modules: StatusModule[];
}

interface StatusDialogProps {
  onClose: () => void;
}

export const StatusDialog = React.memo(function StatusDialog(props: StatusDialogProps) {
  const { onClose } = props;
  const [responseState] = useQueryManager<null, StatusResponse>({
    processQuery: async () => {
      const resp = await Api.instance.get(`/status`);
      return resp.data;
    },
    initQuery: null,
  });

  function renderContent(): JSX.Element | undefined {
    if (responseState.loading) return <Loader />;

    if (responseState.error) {
      return <span>{`Error while loading status: ${responseState.error}`}</span>;
    }

    const response = responseState.data;
    if (!response) return;

    return (
      <div className="main-container">
        <div className="version">
          Version:&nbsp;<strong>{response.version}</strong>
        </div>
        <ReactTable
          data={response.modules}
          columns={[
            {
              columns: [
                {
                  Header: 'Extension name',
                  accessor: 'artifact',
                  width: 200,
                },
                {
                  Header: 'Fully qualified name',
                  accessor: 'name',
                },
                {
                  Header: 'Version',
                  accessor: 'version',
                  width: 200,
                },
              ],
            },
          ]}
          loading={responseState.loading}
          filterable
          defaultFilterMethod={anywhereMatcher}
        />
      </div>
    );
  }

  return (
    <Dialog className="status-dialog" onClose={onClose} isOpen title="Status">
      <div className={Classes.DIALOG_BODY}>{renderContent()}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className="view-raw-button">
          <Button
            text="View raw"
            minimal
            onClick={() => window.open(UrlBaser.base(`/status`), '_blank')}
          />
        </div>
        <div className="close-button">
          <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
});
