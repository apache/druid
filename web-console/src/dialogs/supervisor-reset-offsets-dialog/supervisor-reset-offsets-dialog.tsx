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

import { Button, Classes, Code, ControlGroup, Dialog, FormGroup, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { Loader } from '../../components';
import { FancyNumericInput } from '../../components/fancy-numeric-input/fancy-numeric-input';
import { useQueryManager } from '../../hooks';
import { Api, AppToaster } from '../../singletons';
import { deepDelete, deepGet, getDruidErrorMessage } from '../../utils';

import './supervisor-reset-offsets-dialog.scss';

type OffsetMap = Record<string, number>;

interface SupervisorResetOffsetsDialogProps {
  supervisorId: string;
  supervisorType: string;
  onClose: () => void;
}

export const SupervisorResetOffsetsDialog = React.memo(function SupervisorResetOffsetsDialog(
  props: SupervisorResetOffsetsDialogProps,
) {
  const { supervisorId, supervisorType, onClose } = props;
  const [offsetsToResetTo, setOffsetsToResetTo] = useState<OffsetMap>({});

  const [statusResp] = useQueryManager<string, OffsetMap>({
    initQuery: supervisorId,
    processQuery: async supervisorId => {
      const statusResp = await Api.instance.get(
        `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/status`,
      );
      return statusResp.data;
    },
  });

  const stream = deepGet(statusResp.data || {}, 'payload.stream');
  const latestOffsets = deepGet(statusResp.data || {}, 'payload.latestOffsets');
  const latestOffsetsEntries = latestOffsets ? Object.entries(latestOffsets) : undefined;

  async function onSave() {
    if (!stream) return;
    if (!Object.keys(offsetsToResetTo).length) return;

    try {
      await Api.instance.post(
        `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/resetOffsets`,
        {
          type: supervisorType,
          partitions: {
            type: 'end',
            stream,
            partitionOffsetMap: offsetsToResetTo,
          },
        },
      );
    } catch (e) {
      AppToaster.show({
        message: `Failed to set offsets: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
      return;
    }

    AppToaster.show({
      message: `${supervisorId} offsets have been set`,
      intent: Intent.SUCCESS,
    });
    onClose();
  }

  return (
    <Dialog
      className="supervisor-reset-offsets-dialog"
      isOpen
      onClose={onClose}
      title={`Set supervisor offsets: ${supervisorId}`}
    >
      <div className={Classes.DIALOG_BODY}>
        {statusResp.loading && <Loader />}
        {latestOffsetsEntries && (
          <>
            <p>
              Set <Code>{supervisorId}</Code> to specific offsets
            </p>
            {latestOffsetsEntries.map(([key, latestOffset]) => (
              <FormGroup key={key} label={key} helperText={`(currently: ${latestOffset})`}>
                <ControlGroup>
                  <Button className="label-button" text="New offset:" disabled />
                  <FancyNumericInput
                    value={offsetsToResetTo[key]}
                    onValueChange={valueAsNumber => {
                      setOffsetsToResetTo({ ...offsetsToResetTo, [key]: valueAsNumber });
                    }}
                    onValueEmpty={() => {
                      setOffsetsToResetTo(deepDelete(offsetsToResetTo, key));
                    }}
                    min={0}
                    fill
                    placeholder="Don't change offset"
                  />
                </ControlGroup>
              </FormGroup>
            ))}
            {latestOffsetsEntries.length === 0 && (
              <p>There are no partitions currently in this supervisor.</p>
            )}
          </>
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button text="Save" intent={Intent.PRIMARY} onClick={() => void onSave()} />
        </div>
      </div>
    </Dialog>
  );
});
