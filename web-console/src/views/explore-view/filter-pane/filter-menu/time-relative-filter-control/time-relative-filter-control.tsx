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
  anchor: 'currentTimestamp' | 'maxDataTime';
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

const GROUPS: GroupedNamedPartialPatterns[] = [
  {
    groupName: 'Latest',
    namedPartialPatterns: [
      {
        name: 'Hour',
        partialPattern: {
          anchor: 'maxDataTime',
          rangeDuration: 'PT1H',
        },
      },
      {
        name: 'Day',
        partialPattern: {
          anchor: 'maxDataTime',
          rangeDuration: 'P1D',
        },
      },
      {
        name: 'Week',
        partialPattern: {
          anchor: 'maxDataTime',
          rangeDuration: 'P1W',
        },
      },
    ],
  },
  {
    groupName: 'Current',
    namedPartialPatterns: [
      {
        name: 'Hour',
        partialPattern: {
          anchor: 'currentTimestamp',
          alignType: 'ceil',
          alignDuration: 'PT1H',
          rangeDuration: 'PT1H',
        },
      },
      {
        name: 'Day',
        partialPattern: {
          anchor: 'currentTimestamp',
          alignType: 'ceil',
          alignDuration: 'P1D',
          rangeDuration: 'P1D',
        },
      },
      {
        name: 'Week',
        partialPattern: {
          anchor: 'currentTimestamp',
          alignType: 'ceil',
          alignDuration: 'P1W',
          rangeDuration: 'P1W',
        },
      },
    ],
  },
  {
    groupName: 'Previous',
    namedPartialPatterns: [
      {
        name: 'Hour',
        partialPattern: {
          anchor: 'currentTimestamp',
          alignType: 'floor',
          alignDuration: 'PT1H',
          rangeDuration: 'PT1H',
        },
      },
      {
        name: 'Day',
        partialPattern: {
          anchor: 'currentTimestamp',
          alignType: 'floor',
          alignDuration: 'P1D',
          rangeDuration: 'P1D',
        },
      },
      {
        name: 'Week',
        partialPattern: {
          anchor: 'currentTimestamp',
          alignType: 'floor',
          alignDuration: 'P1W',
          rangeDuration: 'P1W',
        },
      },
    ],
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
                });
              }}
            />
          ))}
        </FormGroup>
      ))}
    </div>
  );
});
