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

import { Button, Classes, Dialog, InputGroup, Intent } from '@blueprintjs/core';
import axios from 'axios';
import React from 'react';
import ReactTable from 'react-table';

import { UrlBaser } from '../../singletons/url-baser';
import { QueryManager } from '../../utils';

import './status-dialog.scss';

interface StatusDialogProps {
  onClose: () => void;
}

interface StatusDialogState {
  response: any;
  loading: boolean;
  error?: string;
  version: string;
}

export class StatusDialog extends React.PureComponent<StatusDialogProps, StatusDialogState> {
  private showStatusQueryManager: QueryManager<null, string>;
  constructor(props: StatusDialogProps, context: any) {
    super(props, context);
    this.state = {
      response: [],
      loading: false,
      version: '',
    };
    this.showStatusQueryManager = new QueryManager({
      processQuery: async () => {
        const endpoint = UrlBaser.base(`/status`);
        const resp = await axios.get(endpoint);
        this.setState({ version: 'Version ' + resp.data.version });
        return resp.data.modules;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          loading,
          error,
          response: result,
        });
      },
    });
  }

  componentDidMount(): void {
    this.showStatusQueryManager.runQuery(null);
  }

  render(): JSX.Element {
    const { onClose } = this.props;
    const { response, loading, version } = this.state;
    return (
      <Dialog className={'status-dialog'} onClose={onClose} isOpen title="Status">
        <div className={'status-dialog-main-area'}>
          <InputGroup defaultValue={version} readOnly />
          <ReactTable
            data={response}
            columns={[
              {
                Header: 'Extensions',
                columns: [
                  {
                    Header: 'Extension Name',
                    accessor: 'artifact',
                    width: 200,
                  },
                  {
                    Header: 'Fully Qualified Name',
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
            loading={loading}
            filterable
          />
        </div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button
              text="View raw"
              disabled={!response}
              minimal
              onClick={() => window.open(UrlBaser.base(UrlBaser.base(`/status`)), '_blank')}
            />
            <Button text="Close" intent={Intent.PRIMARY} onClick={onClose} />
          </div>
        </div>
      </Dialog>
    );
  }
}
