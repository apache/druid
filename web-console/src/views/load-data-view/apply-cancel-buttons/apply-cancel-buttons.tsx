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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import './apply-cancel-buttons.scss';

export interface ApplyCancelButtonsProps {
  onClose: () => void;
  disableApply?: boolean;
  onApply: () => void;
  showDelete?: boolean;
  disableDelete?: boolean;
  onDelete?: () => void;
}

export function ApplyCancelButtons(props: ApplyCancelButtonsProps) {
  const { disableApply, onApply, onClose, showDelete, disableDelete, onDelete } = props;

  return (
    <div className="apply-cancel-buttons">
      <Button
        text="Apply"
        intent={Intent.PRIMARY}
        disabled={disableApply}
        onClick={() => {
          onApply();
          onClose();
        }}
      />
      <Button text="Cancel" onClick={onClose} />
      {showDelete && onDelete && (
        <Button
          className="delete"
          icon={IconNames.TRASH}
          intent={Intent.DANGER}
          disabled={disableDelete}
          onClick={() => {
            onDelete();
            onClose();
          }}
        />
      )}
    </div>
  );
}
