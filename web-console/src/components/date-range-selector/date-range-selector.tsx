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
import React, { useState } from 'react';

import { dateToIsoDateString, localToUtcDate, utcToLocalDate } from '../../utils';

import './date-range-selector.scss';

interface DateRangeSelectorProps {
  startDate: Date;
  endDate: Date;
  onChange: (startDate: Date, endDate: Date) => void;
}

export const DateRangeSelector = React.memo(function DateRangeSelector(
  props: DateRangeSelectorProps,
) {
  const { startDate, endDate, onChange } = props;
  const [intermediateDateRange, setIntermediateDateRange] = useState<DateRange | undefined>();

  return (
    <Popover
      className="date-range-selector"
      content={
        <DateRangePicker
          value={intermediateDateRange || [utcToLocalDate(startDate), utcToLocalDate(endDate)]}
          contiguousCalendarMonths={false}
          reverseMonthAndYearMenus
          onChange={(selectedRange: DateRange) => {
            const [startDate, endDate] = selectedRange;
            if (!startDate || !endDate) {
              setIntermediateDateRange(selectedRange);
            } else {
              setIntermediateDateRange(undefined);
              onChange(localToUtcDate(startDate), localToUtcDate(endDate));
            }
          }}
        />
      }
      position={Position.BOTTOM_RIGHT}
    >
      <InputGroup
        value={`${dateToIsoDateString(startDate)} ➔ ${dateToIsoDateString(endDate)}`}
        readOnly
        rightElement={<Button rightIcon={IconNames.CALENDAR} minimal />}
      />
    </Popover>
  );
});
