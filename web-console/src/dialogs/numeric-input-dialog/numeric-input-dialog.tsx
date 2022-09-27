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

import { Button, Classes, Dialog, Intent, NumericInput } from '@blueprintjs/core';
import React, { useState } from 'react';

const DEFAULT_MIN_VALUE = 1;

interface NumericInputDialogProps {
  title: string;
  message?: JSX.Element;
  minValue?: number;
  initValue: number;
  integer?: boolean;
  onSubmit(value: number): void;
  onClose(): void;
}

export const NumericInputDialog = React.memo(function NumericInputDialog(
  props: NumericInputDialogProps,
) {
  const { title, message, minValue, initValue, integer, onSubmit, onClose } = props;
  const effectiveMinValue = minValue ?? DEFAULT_MIN_VALUE;

  const [valueString, setValueString] = useState<string>(String(initValue));

  function done() {
    let value = Math.max(Number(valueString) || 0, effectiveMinValue);
    if (integer) {
      value = Math.round(value);
    }
    onSubmit(value);
    onClose();
  }

  return (
    <Dialog
      className="numeric-input-dialog"
      onClose={onClose}
      isOpen
      title={title}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        {message}
        <NumericInput
          value={valueString}
          onValueChange={(_, v) => {
            // Constrain to only simple numeric characters
            v = v.replace(/[^\d\-.]/, '');

            if (integer) {
              // If in integer mode throw away the decimal point
              v = v.replace(/\./, '');
            }

            if (effectiveMinValue >= 0) {
              // If in non-negative mode throw away the minus
              v = v.replace(/-/, '');
            }

            setValueString(v);
          }}
          onKeyDown={(e: React.KeyboardEvent<HTMLInputElement>) => {
            if (e.key !== 'Enter') return;
            done();
          }}
          min={effectiveMinValue}
          stepSize={1}
          minorStepSize={null}
          majorStepSize={10}
          fill
          autoFocus
          selectAllOnFocus
          allowNumericCharactersOnly
        />
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button text="OK" intent={Intent.PRIMARY} onClick={done} />
        </div>
      </div>
    </Dialog>
  );
});
