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

import { FormGroup } from '@blueprintjs/core';
import type { TimeIntervalFilterPattern } from 'druid-query-toolkit';
import React, { useState } from 'react';

import type { QuerySource } from '../../../../models';
import { IsoDateInput } from '../../../iso-date-input/iso-date-input';

import './time-interval-filter-control.scss';

function isSwappedFilterPattern(pattern: TimeIntervalFilterPattern) {
  return pattern.end <= pattern.start;
}

export interface TimeIntervalFilterControlProps {
  querySource: QuerySource;
  filterPattern: TimeIntervalFilterPattern;
  setFilterPatternOrIssue(
    filterPattern: TimeIntervalFilterPattern | undefined,
    issue: string | undefined,
  ): void;
  onIssue(issue: string): void;
}

export const TimeIntervalFilterControl = React.memo(function TimeIntervalFilterControl(
  props: TimeIntervalFilterControlProps,
) {
  const { filterPattern, setFilterPatternOrIssue, onIssue } = props;
  const [swappedFilterPattern, setSwappedFilterPattern] = useState<
    TimeIntervalFilterPattern | undefined
  >();
  const { start, end } = swappedFilterPattern || filterPattern;

  return (
    <div className="time-interval-filter-control">
      <FormGroup label="Start">
        <IsoDateInput
          date={start}
          onChange={start => {
            const newPattern = { ...filterPattern, start };
            if (isSwappedFilterPattern(newPattern)) {
              setSwappedFilterPattern(newPattern);
              setFilterPatternOrIssue(undefined, 'Start date must be before end date');
            } else {
              setSwappedFilterPattern(undefined);
              setFilterPatternOrIssue(newPattern, undefined);
            }
          }}
          onIssue={issue => onIssue(`Bad start date: ${issue}`)}
        />
      </FormGroup>
      <FormGroup label="End">
        <IsoDateInput
          date={end}
          onChange={end => {
            const newPattern = { ...filterPattern, end };
            if (isSwappedFilterPattern(newPattern)) {
              setSwappedFilterPattern(newPattern);
              setFilterPatternOrIssue(undefined, 'End date must be after start date');
            } else {
              setSwappedFilterPattern(undefined);
              setFilterPatternOrIssue(newPattern, undefined);
            }
          }}
          onIssue={issue => onIssue(`Bad end date: ${issue}`)}
        />
      </FormGroup>
    </div>
  );
});
