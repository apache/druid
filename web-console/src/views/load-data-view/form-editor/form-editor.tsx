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
import React, { useState } from 'react';

import type { Field } from '../../../components';
import { AutoForm } from '../../../components';

import './form-editor.scss';

export interface FormEditorProps<T> {
  fields: Field<T>[];
  initValue: T;
  showCustom?: (thing: Partial<T>) => boolean;
  onClose: () => void;
  onDirty: () => void;
  onApply: (thing: T) => void;
  showDelete?: boolean;
  disableDelete?: boolean;
  onDelete?: () => void;
  children?: any;
}

export function FormEditor<T extends Record<string, any>>(props: FormEditorProps<T>) {
  const {
    fields,
    initValue,
    showCustom,
    onDirty,
    onApply,
    onClose,
    showDelete,
    disableDelete,
    onDelete,
    children,
  } = props;

  const [currentValue, setCurrentValue] = useState<Partial<T>>(initValue);

  return (
    <div className="form-editor">
      <AutoForm
        fields={fields}
        model={currentValue}
        onChange={m => {
          onDirty();
          setCurrentValue(m);
        }}
        showCustom={showCustom}
      />
      {children}
      <div className="apply-cancel-buttons">
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
        <Button text="Cancel" onClick={onClose} />
        <Button
          text="Apply"
          intent={Intent.PRIMARY}
          disabled={currentValue === initValue || !AutoForm.isValidModel(currentValue, fields)}
          onClick={() => {
            onApply(currentValue as T);
            onClose();
          }}
        />
      </div>
    </div>
  );
}
