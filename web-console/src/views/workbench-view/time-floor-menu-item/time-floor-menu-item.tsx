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

import { MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression, SqlFunction, SqlLiteral } from 'druid-query-toolkit';
import React from 'react';

import { compact, oneOf, tickIcon } from '../../../utils';

const OPTIONS: { label: string; value: string }[] = [
  { label: 'Second', value: 'PT1S' },
  { label: 'Minute', value: 'PT1M' },
  { label: 'Hour', value: 'PT1H' },
  { label: 'Day', value: 'P1D' },
  { label: 'Month', value: 'P1M' },
  { label: 'Year', value: 'P1Y' },
];

const UNIT_TO_DURATION: Record<string, string> = {
  SECOND: 'PT1S',
  MINUTE: 'PT1M',
  HOUR: 'PT1H',
  DAY: 'P1D',
  WEEK: 'P7D',
  MONTH: 'P1M',
  QUARTER: 'P3M',
  YEAR: 'P1Y',
};

export interface TimeFloorMenuItemProps {
  expression: SqlExpression;
  onChange(expression: SqlExpression): void;
}

export const TimeFloorMenuItem = function TimeFloorMenuItem(props: TimeFloorMenuItemProps) {
  const { expression, onChange } = props;

  let innerExpression = expression.getUnderlyingExpression();
  let currentDuration: string | undefined;
  let origin: SqlExpression | undefined;
  let timezone: SqlExpression | undefined;
  if (
    innerExpression instanceof SqlFunction &&
    oneOf(innerExpression.getEffectiveFunctionName(), 'TIME_FLOOR', 'FLOOR')
  ) {
    const firstArg = innerExpression.getArg(0);
    const secondArg = innerExpression.getArgAsString(1)?.toUpperCase();
    origin = innerExpression.getArg(2);
    timezone = innerExpression.getArg(3);
    if (firstArg && secondArg) {
      innerExpression = firstArg;
      currentDuration = UNIT_TO_DURATION[secondArg] || secondArg;
    }
  }

  const changeTimeFloor = (duration: string | undefined) => {
    onChange(
      (duration
        ? SqlFunction.simple(
            'TIME_FLOOR',
            compact([innerExpression, SqlLiteral.create(duration), origin, timezone]),
          )
        : innerExpression
      ).as(expression.getOutputName()),
    );
  };

  return (
    <MenuItem icon={IconNames.TIME} text="Floor time">
      <MenuItem
        icon={tickIcon(!currentDuration)}
        text="None"
        onClick={() => changeTimeFloor(undefined)}
      />
      {OPTIONS.map(({ label, value }) => (
        <MenuItem
          key={value}
          icon={tickIcon(currentDuration === value)}
          text={label}
          label={value}
          onClick={() => changeTimeFloor(value)}
        />
      ))}
    </MenuItem>
  );
};
