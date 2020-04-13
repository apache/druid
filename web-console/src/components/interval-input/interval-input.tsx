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

import { Button, InputGroup, Popover, Position } from '@blueprintjs/core';
import { DateRange, DateRangePicker } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import './interval-input.scss';

const CURRENT_YEAR = new Date().getUTCFullYear();

function removeLocalTimezone(localDate: Date): Date {
  // Function removes the local timezone of the date and displays it in UTC
  return new Date(localDate.getTime() - localDate.getTimezoneOffset() * 60000);
}

function parseInterval(interval: string): DateRange {
  const dates = interval.split('/');
  if (dates.length !== 2) {
    return [undefined, undefined];
  }
  const startDate = Date.parse(dates[0]) ? new Date(dates[0]) : undefined;
  const endDate = Date.parse(dates[1]) ? new Date(dates[1]) : undefined;
  // Must check if the start and end dates are within range
  return [
    startDate && startDate.getFullYear() < CURRENT_YEAR - 20 ? undefined : startDate,
    endDate && endDate.getFullYear() > CURRENT_YEAR ? undefined : endDate,
  ];
}
function stringifyDateRange(localRange: DateRange): string {
  // This function takes in the dates selected from datepicker in local time, and displays them in UTC
  // Shall Blueprint make any changes to the way dates are selected, this function will have to be reworked
  const [localStartDate, localEndDate] = localRange;
  return `${
    localStartDate
      ? removeLocalTimezone(localStartDate)
          .toISOString()
          .substring(0, 19)
      : ''
  }/${
    localEndDate
      ? removeLocalTimezone(localEndDate)
          .toISOString()
          .substring(0, 19)
      : ''
  }`;
}

export interface IntervalInputProps {
  interval: string;
  placeholder: string | undefined;
  onValueChange: (interval: string) => void;
}

export const IntervalInput = React.memo(function IntervalInput(props: IntervalInputProps) {
  const { interval, placeholder, onValueChange } = props;

  return (
    <InputGroup
      value={interval}
      placeholder={placeholder}
      rightElement={
        <div>
          <Popover
            popoverClassName={'calendar'}
            content={
              <DateRangePicker
                timePrecision={'second'}
                value={parseInterval(interval)}
                contiguousCalendarMonths={false}
                onChange={(selectedRange: DateRange) => {
                  onValueChange(stringifyDateRange(selectedRange));
                }}
              />
            }
            position={Position.BOTTOM_RIGHT}
          >
            <Button rightIcon={IconNames.CALENDAR} />
          </Popover>
        </div>
      }
      onChange={(e: any) => {
        const value = e.target.value.replace(/[^\-0-9T:/]/g, '').substring(0, 39);
        onValueChange(value);
      }}
    />
  );
});
