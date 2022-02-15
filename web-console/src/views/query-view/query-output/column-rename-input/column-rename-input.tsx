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

import { InputGroup } from '@blueprintjs/core';
import React, { useState } from 'react';

export interface ColumnRenameInputProps {
  initialName: string;
  onDone: (newName?: string) => void;
}

export const ColumnRenameInput = React.memo(function ColumnRenameInput(
  props: ColumnRenameInputProps,
) {
  const { initialName, onDone } = props;
  const [newName, setNewName] = useState<string>(initialName);

  function maybeDone() {
    if (newName && newName !== initialName) {
      onDone(newName);
    } else {
      onDone();
    }
  }

  return (
    <InputGroup
      className="column-rename-input"
      value={newName}
      onChange={(e: any) => setNewName(e.target.value)}
      onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
        switch (e.key) {
          case 'Enter':
            maybeDone();
            break;

          case 'Escape':
            onDone();
            break;
        }
      }}
      onBlur={maybeDone}
      small
      autoFocus
    />
  );
});
