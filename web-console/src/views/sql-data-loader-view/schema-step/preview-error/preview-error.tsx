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

import { Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import './preview-error.scss';

interface PreviewErrorProps {
  errorMessage: string;
  onRevert?: () => void;
}

export const PreviewError = function PreviewError(props: PreviewErrorProps) {
  const { errorMessage, onRevert } = props;

  return (
    <div className="preview-error">
      <div className="error-message">{errorMessage}</div>
      {onRevert && (
        <Button icon={IconNames.UNDO} text="Revert to last working state" onClick={onRevert} />
      )}
    </div>
  );
};
