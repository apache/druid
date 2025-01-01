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

import {
  Button,
  Classes,
  ControlGroup,
  Dialog,
  FormGroup,
  Intent,
  Label,
  Tag,
} from '@blueprintjs/core';
import React, { useState } from 'react';

import type { FormJsonTabs } from '../../components';
import { FormJsonSelector, JsonInput, Loader } from '../../components';
import { FancyNumericInput } from '../../components/fancy-numeric-input/fancy-numeric-input';
import type { SupervisorOffsetMap, SupervisorStatus } from '../../druid-models';
import { useQueryManager } from '../../hooks';
import { Api, AppToaster } from '../../singletons';
import {
  deepDelete,
  deepGet,
  formatInteger,
  getDruidErrorMessage,
  isNumberLike,
} from '../../utils';

import './supervisor-reset-offsets-dialog.scss';

function numberOrUndefined(x: any): number | undefined {
  if (typeof x === 'undefined') return;
  return Number(x);
}

interface PartitionEntry {
  partition: string;
  currentOffset?: number;
}

function getPartitionEntries(
  supervisorStatus: SupervisorStatus,
  partitionOffsetMap: SupervisorOffsetMap,
): PartitionEntry[] {
  const latestOffsets = supervisorStatus.payload?.latestOffsets;
  const minimumLag = supervisorStatus.payload?.minimumLag;
  let partitions: PartitionEntry[];
  if (latestOffsets && minimumLag) {
    partitions = Object.entries(latestOffsets).map(([partition, latestOffset]) => {
      return {
        partition,
        currentOffset: Number(latestOffset) - Number(minimumLag[partition] || 0),
      };
    });
  } else {
    partitions = [];
    const numPartitions = supervisorStatus.payload?.partitions;
    for (let p = 0; p < numPartitions; p++) {
      partitions.push({ partition: String(p) });
    }
  }

  Object.keys(partitionOffsetMap).forEach(p => {
    if (partitions.some(({ partition }) => partition === p)) return;
    partitions.push({ partition: p });
  });

  partitions.sort((a, b) => {
    return a.partition.localeCompare(b.partition, undefined, { numeric: true });
  });

  return partitions;
}

interface SupervisorResetOffsetsDialogProps {
  supervisorId: string;
  supervisorType: string;
  onClose(): void;
}

export const SupervisorResetOffsetsDialog = React.memo(function SupervisorResetOffsetsDialog(
  props: SupervisorResetOffsetsDialogProps,
) {
  const { supervisorId, supervisorType, onClose } = props;
  const [partitionOffsetMap, setPartitionOffsetMap] = useState<SupervisorOffsetMap>({});
  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [jsonError, setJsonError] = useState<Error | undefined>();
  const disableSubmit = Boolean(jsonError);

  const [statusResp] = useQueryManager<string, SupervisorStatus>({
    initQuery: supervisorId,
    processQuery: async (supervisorId, cancelToken) => {
      return (
        await Api.instance.get(
          `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/status`,
          { cancelToken },
        )
      ).data;
    },
  });

  // Kafka:   Topic, Partition, Offset
  // Kinesis: Stream, Shard, Sequence number
  const partitionLabel = supervisorType === 'kinesis' ? 'Shard' : 'Partition';
  const offsetLabel = supervisorType === 'kinesis' ? 'sequence number' : 'offset';

  async function onSubmit() {
    const stream = deepGet(statusResp.data || {}, 'payload.stream');
    if (!stream) return;
    if (!Object.keys(partitionOffsetMap).length) return;

    try {
      await Api.instance.post(
        `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/resetOffsets`,
        {
          type: supervisorType,
          partitions: {
            type: 'end',
            stream,
            partitionOffsetMap,
          },
        },
      );
    } catch (e) {
      AppToaster.show({
        message: `Failed to set ${offsetLabel}s: ${getDruidErrorMessage(e)}`,
        intent: Intent.DANGER,
      });
      return;
    }

    AppToaster.show({
      message: (
        <>
          <Tag minimal>{supervisorId}</Tag> {offsetLabel}s have been set.
        </>
      ),
      intent: Intent.SUCCESS,
    });
    onClose();
  }

  const supervisorStatus = statusResp.data;
  return (
    <Dialog
      className="supervisor-reset-offsets-dialog"
      isOpen
      onClose={onClose}
      title={`Set supervisor ${offsetLabel}s`}
    >
      <div className={Classes.DIALOG_BODY}>
        <p>
          Set <Tag minimal>{supervisorId}</Tag> to read from specific {offsetLabel}s.
        </p>
        <FormJsonSelector
          tab={currentTab}
          onChange={t => {
            setJsonError(undefined);
            setCurrentTab(t);
          }}
        />
        {currentTab === 'form' ? (
          <>
            {statusResp.loading && <Loader />}
            {supervisorStatus &&
              getPartitionEntries(supervisorStatus, partitionOffsetMap).map(
                ({ partition, currentOffset }) => (
                  <FormGroup
                    key={partition}
                    label={`${partitionLabel} ${partition}${
                      typeof currentOffset === 'undefined'
                        ? ''
                        : ` (current ${offsetLabel}=${formatInteger(currentOffset)})`
                    }:`}
                  >
                    <ControlGroup>
                      <Label className="new-offset-label">{`New ${offsetLabel}:`}</Label>
                      <FancyNumericInput
                        value={numberOrUndefined(partitionOffsetMap[partition])}
                        onValueChange={valueAsNumber => {
                          setPartitionOffsetMap({
                            ...partitionOffsetMap,
                            [partition]: valueAsNumber,
                          });
                        }}
                        onValueEmpty={() => {
                          setPartitionOffsetMap(deepDelete(partitionOffsetMap, partition));
                        }}
                        min={0}
                        fill
                        placeholder={`Don't change ${offsetLabel}`}
                      />
                    </ControlGroup>
                  </FormGroup>
                ),
              )}
          </>
        ) : (
          <JsonInput
            value={partitionOffsetMap}
            onChange={setPartitionOffsetMap}
            setError={setJsonError}
            issueWithValue={value => {
              if (!value || typeof value !== 'object') {
                return `The ${offsetLabel} map must be an object`;
              }
              const badValue = Object.entries(value).find(([_, v]) => !isNumberLike(v));
              if (badValue) {
                return `The value of ${badValue[0]} is not a number`;
              }
              return;
            }}
            height="300px"
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            disabled={disableSubmit}
            onClick={() => void onSubmit()}
          />
        </div>
      </div>
    </Dialog>
  );
});
