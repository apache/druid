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
import React from 'react';

import type { QuerySource } from '../../../../models';
import { UtcDateInput } from '../../../utc-date-input/utc-date-input';

import './time-interval-filter-control.scss';

export interface TimeIntervalFilterControlProps {
  querySource: QuerySource;
  filterPattern: TimeIntervalFilterPattern;
  setFilterPattern(filterPattern: TimeIntervalFilterPattern): void;
}

export const TimeIntervalFilterControl = React.memo(function TimeIntervalFilterControl(
  props: TimeIntervalFilterControlProps,
) {
  const { filterPattern, setFilterPattern } = props;
  const { start, end } = filterPattern;

  return (
    <div className="time-interval-filter-control">
      <FormGroup label="Start">
        <UtcDateInput
          date={start}
          onChange={start => setFilterPattern({ ...filterPattern, start })}
        />
      </FormGroup>
      <FormGroup label="End">
        <UtcDateInput date={end} onChange={end => setFilterPattern({ ...filterPattern, end })} />
      </FormGroup>
    </div>
  );
});
