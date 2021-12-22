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

import { downloadFile } from '../../utils';

import './show-value.scss';

export interface ShowValueProps {
  jsonValue: string;
  onDiffWithPrevious?: () => void;
  downloadFilename?: string;
}

export const ShowValue = React.memo(function ShowValue(props: ShowValueProps) {
  const { jsonValue, onDiffWithPrevious, downloadFilename } = props;
  return (
    <div className="show-json">
      {(onDiffWithPrevious || downloadFilename) && (
        <div className="top-actions">
          <ButtonGroup className="right-buttons">
            {onDiffWithPrevious && (
              <Button text="Diff with previous" minimal onClick={onDiffWithPrevious} />
            )}
            {downloadFilename && (
              <Button
                text="Save"
                minimal
                onClick={() => downloadFile(jsonValue, 'json', downloadFilename)}
              />
            )}
          </ButtonGroup>
        </div>
      )}
      <div className="main-area">
        <TextArea readOnly value={jsonValue} />
      </div>
    </div>
  );
});
