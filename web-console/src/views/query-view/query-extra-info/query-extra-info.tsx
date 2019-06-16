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

import { Button, Menu, MenuItem, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import './query-extra-info.scss';

export interface QueryExtraInfoData {
  id: string;
  elapsed: number;
}

export interface QueryExtraInfoProps extends React.Props<any> {
  queryExtraInfo: QueryExtraInfoData;
  onDownload: (format: string) => void;
}

export class QueryExtraInfo extends React.PureComponent<QueryExtraInfoProps> {

  render() {
    const { queryExtraInfo, onDownload } = this.props;

    const downloadMenu = <Menu className="download-format-menu">
      <MenuItem text="CSV" onClick={() => onDownload('csv')} />
      <MenuItem text="TSV" onClick={() => onDownload('tsv')} />
      <MenuItem text="JSON (new line delimited)" onClick={() => onDownload('json')}/>
    </Menu>;

    return <div className="query-extra-info">
      <div className="query-elapsed">
        {`Last query took ${(queryExtraInfo.elapsed / 1000).toFixed(2)} seconds`}
      </div>
      <Popover className="download-button" content={downloadMenu} position={Position.BOTTOM_RIGHT}>
        <Button
          icon={IconNames.DOWNLOAD}
          minimal
        />
      </Popover>
    </div>;
  }
}
