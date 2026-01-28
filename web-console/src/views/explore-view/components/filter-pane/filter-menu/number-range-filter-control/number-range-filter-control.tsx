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

import { Button, ControlGroup, FormGroup, NumericInput } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { NumberRangeFilterPattern } from 'druid-query-toolkit';
import React from 'react';

import type { QuerySource } from '../../../../models';

export interface NumberRangeFilterControlProps {
  querySource: QuerySource;
  filterPattern: NumberRangeFilterPattern;
  setFilterPattern(filterPattern: NumberRangeFilterPattern): void;
}

export const NumberRangeFilterControl = React.memo(function NumberRangeFilterControl(
  props: NumberRangeFilterControlProps,
) {
  const { filterPattern, setFilterPattern } = props;
  const { start, startBound, end, endBound } = filterPattern;

  return (
    <div className="number-range-filter-control">
      <FormGroup label="Bounds">
        <ControlGroup>
          <NumericInput
            value={start}
            onValueChange={start => setFilterPattern({ ...filterPattern, start })}
            fill
          />
          <Button
            icon={startBound === '[' ? IconNames.LESS_THAN_OR_EQUAL_TO : IconNames.LESS_THAN}
            onClick={() =>
              setFilterPattern({ ...filterPattern, startBound: startBound === '[' ? '(' : '[' })
            }
          />
          <Button text="X" disabled />
          <Button
            icon={endBound === ']' ? IconNames.LESS_THAN_OR_EQUAL_TO : IconNames.LESS_THAN}
            onClick={() =>
              setFilterPattern({ ...filterPattern, endBound: endBound === ']' ? ')' : ']' })
            }
          />
          <NumericInput
            value={end}
            onValueChange={end => setFilterPattern({ ...filterPattern, end })}
            fill
          />
        </ControlGroup>
      </FormGroup>
    </div>
  );
});
