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

import { Button, ButtonGroup, TextArea } from '@blueprintjs/core';
import React from 'react';

import { UrlBaser } from '../../singletons/url-baser';
import { downloadFile } from '../../utils';

import './show-value.scss';

export interface ShowValueProps {
  endpoint?: string;
  downloadFilename?: string;
  jsonValue?: string;
}

export function ShowValue(props: ShowValueProps) {
  const { endpoint, downloadFilename, jsonValue } = props;
  return (
    <div className="show-json">
      <div className="top-actions">
        <ButtonGroup className="right-buttons">
          {downloadFilename && (
            <Button
              text="Save"
              minimal
              onClick={() => (jsonValue ? downloadFile(jsonValue, 'json', downloadFilename) : null)}
            />
          )}
          {endpoint && (
            <Button
              text="View raw"
              minimal
              onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
            />
          )}
        </ButtonGroup>
      </div>
      <div className="main-area">
        <TextArea readOnly value={jsonValue} />
      </div>
    </div>
  );
}
