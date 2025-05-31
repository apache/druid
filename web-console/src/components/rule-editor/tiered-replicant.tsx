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

import { Button, ControlGroup, NumericInput } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { SuggestibleInput } from '../suggestible-input/suggestible-input';

export interface TieredReplicantProps {
  tier: string;
  replication: number;
  onChangeTier: (newTier: string) => void;
  onChangeReplication: (value: number) => void;
  tiers: string[];
  usedTiers: string[];
  disabled: boolean;
  onRemove?: () => void;
}

export const TieredReplicant = React.memo(function TieredReplicant(props: TieredReplicantProps) {
  const {
    tier,
    replication,
    tiers,
    usedTiers,
    disabled,
    onChangeReplication,
    onChangeTier,
    onRemove,
  } = props;

  return (
    <ControlGroup className="tiered-replicant">
      <Button minimal disabled={disabled} style={{ pointerEvents: 'none' }}>
        Tier:
      </Button>
      <SuggestibleInput
        fill
        value={tier}
        disabled={disabled}
        onValueChange={value => onChangeTier(value || '')}
        suggestions={tiers.filter(t => t === tier || !usedTiers.includes(t))}
      />
      <Button minimal disabled={disabled} style={{ pointerEvents: 'none' }}>
        Replicants:
      </Button>
      <NumericInput
        value={replication}
        disabled={disabled}
        onValueChange={(v: number) => {
          if (isNaN(v)) return;
          onChangeReplication(v);
        }}
        min={0}
        max={256}
      />
      {onRemove && <Button onClick={onRemove} icon={IconNames.TRASH} />}
    </ControlGroup>
  );
});
