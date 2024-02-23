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

import { Button, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import type { TimeIntervalFilterPattern } from '@druid-toolkit/query';
import React, { useState } from 'react';

import { ColumnPicker } from '../../../column-picker/column-picker';
import type { Dataset } from '../../../utils';

function utcParseDate(dateString: string): Date | undefined {
  const dateParts = dateString.split(/[-T:. ]/g);

  // Extract the individual date and time components
  const year = parseInt(dateParts[0], 10);
  if (!(1000 < year && year < 4000)) return;

  const month = parseInt(dateParts[1], 10);
  if (month > 12) return;

  const day = parseInt(dateParts[2], 10);
  if (day > 31) return;

  const hour = parseInt(dateParts[3], 10);
  if (hour > 23) return;

  const minute = parseInt(dateParts[4], 10);
  if (minute > 59) return;

  const second = parseInt(dateParts[5], 10);
  if (second > 59) return;

  const millisecond = parseInt(dateParts[6], 10);
  if (millisecond >= 1000) return;

  return new Date(Date.UTC(year, month - 1, day, hour, minute, second)); // Month is zero-based
}

function normalizeDateString(dateString: string): string {
  return dateString.replace(/[^\-0-9T:./Z ]/g, '');
}

function formatDate(date: Date): string {
  return date.toISOString().replace(/Z$/, '').replace('.000', '').replace(/T/g, ' ');
}

export interface TimeIntervalFilterControlProps {
  dataset: Dataset;
  initFilterPattern: TimeIntervalFilterPattern;
  negated: boolean;
  setFilterPattern(filterPattern: TimeIntervalFilterPattern): void;
}

export const TimeIntervalFilterControl = React.memo(function TimeIntervalFilterControl(
  props: TimeIntervalFilterControlProps,
) {
  const { dataset, initFilterPattern, negated, setFilterPattern } = props;
  const [column, setColumn] = useState<string>(initFilterPattern.column);
  const [startString, setStartString] = useState<string>(formatDate(initFilterPattern.start));
  const [endString, setEndString] = useState<string>(formatDate(initFilterPattern.end));

  function makePattern(): TimeIntervalFilterPattern | undefined {
    const start = utcParseDate(startString);
    if (!start) return;

    const end = utcParseDate(endString);
    if (!end) return;

    return {
      type: 'timeInterval',
      negated,
      column,
      start,
      end,
    };
  }

  return (
    <div className="time-interval-filter-control">
      <FormGroup label="Column">
        <ColumnPicker
          availableColumns={dataset.columns}
          selectedColumnName={column}
          onSelectedColumnNameChange={setColumn}
        />
      </FormGroup>
      <FormGroup label="Start">
        <InputGroup
          value={startString}
          onChange={e => setStartString(normalizeDateString(e.target.value))}
          placeholder="2022-02-01 00:00:00"
        />
      </FormGroup>
      <FormGroup label="End">
        <InputGroup
          value={endString}
          onChange={e => setEndString(normalizeDateString(e.target.value))}
          placeholder="2022-02-01 00:00:00"
        />
      </FormGroup>
      <div className="button-bar">
        <Button
          intent={Intent.PRIMARY}
          text="OK"
          onClick={() => {
            const newPattern = makePattern();
            if (!newPattern) return;
            setFilterPattern(newPattern);
          }}
        />
      </div>
    </div>
  );
});
