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
import React, { useState } from 'react';

export interface StringInputDialogProps {
  title: string;
  initValue?: string;
  placeholder?: string;
  maxLength?: number;
  onSubmit(str: string): void;
  onClose(): void;
}

export const StringInputDialog = React.memo(function StringSubmitDialog(
  props: StringInputDialogProps,
) {
  const { title, initValue, placeholder, maxLength, onSubmit, onClose } = props;

  const [value, setValue] = useState(initValue || '');

  function handleSubmit() {
    onSubmit(value);
    onClose();
  }

  return (
    <Dialog className="string-input-dialog" isOpen onClose={onClose} title={title}>
      <div className={Classes.DIALOG_BODY}>
        <InputGroup
          value={value}
          onChange={e => setValue(String(e.target.value).substring(0, maxLength || 280))}
          autoFocus
          placeholder={placeholder}
        />
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button text="Submit" intent={Intent.PRIMARY} onClick={handleSubmit} />
        </div>
      </div>
    </Dialog>
  );
});
