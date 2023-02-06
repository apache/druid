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

import { Button, InputGroup, Intent, Position } from '@blueprintjs/core';
import { DateRange, DateRangePicker, TimePrecision } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React from 'react';

import { intervalToLocalDateRange, localDateRangeToInterval } from '../../utils';

export interface IntervalInputProps {
  interval: string;
  placeholder: string | undefined;
  onValueChange: (interval: string) => void;
  intent?: Intent;
}

export const IntervalInput = React.memo(function IntervalInput(props: IntervalInputProps) {
  const { interval, placeholder, onValueChange, intent } = props;

  return (
    <InputGroup
      value={interval}
      placeholder={placeholder}
      rightElement={
        <div>
          <Popover2
            popoverClassName="calendar"
            content={
              <DateRangePicker
                timePrecision={TimePrecision.SECOND}
                value={intervalToLocalDateRange(interval)}
                contiguousCalendarMonths={false}
                reverseMonthAndYearMenus
                onChange={(selectedRange: DateRange) => {
                  onValueChange(localDateRangeToInterval(selectedRange));
                }}
              />
            }
            position={Position.BOTTOM_RIGHT}
          >
            <Button rightIcon={IconNames.CALENDAR} />
          </Popover2>
        </div>
      }
      onChange={(e: any) => {
        const value = e.target.value.replace(/[^\-\dT:/]/g, '').substring(0, 39);
        onValueChange(value);
      }}
      intent={intent}
    />
  );
});
