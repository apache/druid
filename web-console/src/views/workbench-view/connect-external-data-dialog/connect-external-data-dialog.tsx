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

import { Classes, Dialog } from '@blueprintjs/core';
import type { SqlExpression } from '@druid-toolkit/query';
import React, { useState } from 'react';

import type { ArrayMode, ExternalConfig, InputFormat, InputSource } from '../../../druid-models';
import { InputFormatStep } from '../input-format-step/input-format-step';
import { InputSourceStep } from '../input-source-step/input-source-step';

import './connect-external-data-dialog.scss';

export interface ConnectExternalDataDialogProps {
  initExternalConfig?: Partial<ExternalConfig>;
  onSetExternalConfig(
    config: ExternalConfig,
    timeExpression: SqlExpression | undefined,
    partitionedByHint: string | undefined,
    arrayMode: ArrayMode,
  ): void;
  onClose(): void;
}

interface ExternalConfigStep {
  inputSource?: InputSource;
  inputFormat?: InputFormat;
  partitionedByHint?: string;
}

export const ConnectExternalDataDialog = React.memo(function ConnectExternalDataDialog(
  props: ConnectExternalDataDialogProps,
) {
  const { initExternalConfig, onClose, onSetExternalConfig } = props;

  const [externalConfigStep, setExternalConfigStep] = useState<ExternalConfigStep>(
    initExternalConfig || {},
  );

  const { inputSource, inputFormat, partitionedByHint } = externalConfigStep;

  return (
    <Dialog
      className="connect-external-data-dialog"
      isOpen
      onClose={onClose}
      title={`Connect external data / ${inputFormat ? 'Parse' : 'Select input type'}`}
    >
      <div className={Classes.DIALOG_BODY}>
        {inputFormat && inputSource ? (
          <InputFormatStep
            initInputSource={inputSource}
            initInputFormat={inputFormat}
            doneButton
            onSet={({ inputSource, inputFormat, signature, timeExpression, arrayMode }) => {
              onSetExternalConfig(
                { inputSource, inputFormat, signature },
                timeExpression,
                partitionedByHint,
                arrayMode,
              );
              onClose();
            }}
            onBack={() => {
              setExternalConfigStep({ inputSource });
            }}
          />
        ) : (
          <InputSourceStep
            initInputSource={inputSource}
            mode="sampler"
            onSet={(inputSource, inputFormat, partitionedByHint) => {
              setExternalConfigStep({ inputSource, inputFormat, partitionedByHint });
            }}
          />
        )}
      </div>
    </Dialog>
  );
});
