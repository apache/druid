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

import { Button, FormGroup } from '@blueprintjs/core';
import type { TimeRelativeFilterPattern } from '@druid-toolkit/query';
import React, { useState } from 'react';

import { ColumnPicker } from '../../../column-picker/column-picker';
import type { Dataset } from '../../../utils';

interface PartialPattern {
  anchor: 'timestamp' | 'maxDataTime';
  rangeDuration: string;
  alignType?: 'floor' | 'ceil';
  alignDuration?: string;
  shiftDuration?: string;
  shiftStep?: number;
}

function partialPatternToKey(partialPattern: PartialPattern): string {
  return [
    partialPattern.anchor,
    partialPattern.rangeDuration,
    partialPattern.alignType || '-',
    partialPattern.alignDuration || '-',
    partialPattern.shiftDuration || '-',
    partialPattern.shiftStep || '-',
  ].join(',');
}

interface NamedPartialPattern {
  name: string;
  partialPattern: PartialPattern;
}

interface GroupedNamedPartialPatterns {
  groupName: string;
  namedPartialPatterns: NamedPartialPattern[];
}

const DURATIONS_TO_SHOW: [string, string][] = [
  ['Hour', 'PT1H'],
  ['Day', 'P1D'],
  ['Week', 'P1W'],
  ['Month', 'P1M'],
  ['Year', 'P1Y'],
];

const GROUPS: GroupedNamedPartialPatterns[] = [
  {
    groupName: 'Latest',
    namedPartialPatterns: DURATIONS_TO_SHOW.map(([name, duration]) => ({
      name,
      partialPattern: {
        anchor: 'maxDataTime',
        rangeDuration: duration,
      },
    })),
  },
  {
    groupName: 'Current',
    namedPartialPatterns: DURATIONS_TO_SHOW.map(([name, duration]) => ({
      name,
      partialPattern: {
        anchor: 'timestamp',
        alignType: 'ceil',
        alignDuration: duration,
        rangeDuration: duration,
      },
    })),
  },
  {
    groupName: 'Previous',
    namedPartialPatterns: DURATIONS_TO_SHOW.map(([name, duration]) => ({
      name,
      partialPattern: {
        anchor: 'timestamp',
        alignType: 'floor',
        alignDuration: duration,
        rangeDuration: duration,
      },
    })),
  },
];

export interface TimeRelativeFilterControlProps {
  dataset: Dataset;
  initFilterPattern: TimeRelativeFilterPattern;
  negated: boolean;
  setFilterPattern(filterPattern: TimeRelativeFilterPattern): void;
}

export const TimeRelativeFilterControl = React.memo(function TimeRelativeFilterControl(
  props: TimeRelativeFilterControlProps,
) {
  const { dataset, initFilterPattern, negated, setFilterPattern } = props;
  const [column, setColumn] = useState<string>(initFilterPattern.column);

  const initKey = partialPatternToKey(initFilterPattern);
  return (
    <div className="time-relative-filter-control">
      <FormGroup label="Column">
        <ColumnPicker
          availableColumns={dataset.columns}
          selectedColumnName={column}
          onSelectedColumnNameChange={setColumn}
        />
      </FormGroup>
      {GROUPS.map(({ groupName, namedPartialPatterns }, i) => (
        <FormGroup key={i} label={groupName}>
          {namedPartialPatterns.map(({ name, partialPattern }, i) => (
            <Button
              key={i}
              text={name}
              active={initKey === partialPatternToKey(partialPattern)}
              onClick={() => {
                setFilterPattern({
                  type: 'timeRelative',
                  negated,
                  column,
                  ...partialPattern,
                  startBound: '[',
                  endBound: ')',
                });
              }}
            />
          ))}
        </FormGroup>
      ))}
    </div>
  );
});
