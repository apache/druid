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

import type { InputGroupProps2, Intent } from '@blueprintjs/core';
import { Button, ButtonGroup, Classes, ControlGroup, InputGroup, Keys } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression, SqlFunction, SqlLiteral, SqlMulti } from '@druid-toolkit/query';
import classNames from 'classnames';
import React, { useEffect, useState } from 'react';

import { clamp } from '../../utils';

const MULTI_OP_TO_REDUCER: Record<string, (a: number, b: number) => number> = {
  '+': (a, b) => a + b,
  '-': (a, b) => a - b,
  '*': (a, b) => a * b,
  '/': (a, b) => (b ? a / b : 0),
};

function evaluateSqlSimple(sql: SqlExpression): number | undefined {
  if (sql instanceof SqlLiteral) {
    return sql.getNumberValue();
  } else if (sql instanceof SqlMulti) {
    const evaluatedArgs = sql.getArgArray().map(evaluateSqlSimple);
    if (evaluatedArgs.some(x => typeof x === 'undefined')) return;
    const reducer = MULTI_OP_TO_REDUCER[sql.op];
    if (!reducer) return;
    return (evaluatedArgs as number[]).reduce(reducer);
  } else if (sql instanceof SqlFunction && sql.getEffectiveFunctionName() === 'PI') {
    return Math.PI;
  } else {
    return;
  }
}

function numberToShown(n: number | undefined): string {
  if (typeof n === 'undefined') return '';
  return String(n);
}

function shownToNumber(s: string): number | undefined {
  const parsed = SqlExpression.maybeParse(s);
  if (!parsed) return;
  return evaluateSqlSimple(parsed);
}

export interface FancyNumericInputProps {
  className?: string;
  intent?: Intent;
  fill?: boolean;
  large?: boolean;
  small?: boolean;
  disabled?: boolean;
  readOnly?: boolean;
  placeholder?: string;
  onBlur?: InputGroupProps2['onBlur'];

  value: number | undefined;
  defaultValue?: number;
  onValueChange(value: number): void;
  onValueEmpty?: () => void;

  min?: number;
  max?: number;
  minorStepSize?: number;
  stepSize?: number;
  majorStepSize?: number;
  arbitraryPrecision?: boolean;
}

export const FancyNumericInput = React.memo(function FancyNumericInput(
  props: FancyNumericInputProps,
) {
  const {
    className,
    intent,
    fill,
    large,
    small,
    disabled,
    readOnly,
    placeholder,
    onBlur,

    value,
    defaultValue,
    onValueChange,
    onValueEmpty,

    min,
    max,
    arbitraryPrecision,
  } = props;

  const stepSize = props.stepSize || 1;
  const minorStepSize = props.minorStepSize || stepSize;
  const majorStepSize = props.majorStepSize || stepSize * 10;

  function roundAndClamp(n: number): number {
    if (!arbitraryPrecision) {
      const inv = 1 / minorStepSize;
      n = Math.floor(n * inv) / inv;
    }
    return clamp(n, min, max);
  }

  const effectiveValue = value ?? defaultValue;
  const [shownValue, setShownValue] = useState<string>(numberToShown(effectiveValue));
  const shownNumberRaw = shownToNumber(shownValue);
  const shownNumberClamped = shownNumberRaw ? roundAndClamp(shownNumberRaw) : undefined;

  useEffect(() => {
    if (effectiveValue !== shownNumberClamped) {
      setShownValue(numberToShown(effectiveValue));
    }
  }, [effectiveValue]);

  const containerClasses = classNames(
    'fancy-numeric-input',
    Classes.NUMERIC_INPUT,
    { [Classes.LARGE]: large, [Classes.SMALL]: small },
    className,
  );

  const effectiveDisabled = disabled || readOnly;
  const isIncrementDisabled = max !== undefined && value !== undefined && +value >= max;
  const isDecrementDisabled = min !== undefined && value !== undefined && +value <= min;

  function changeValue(newValue: number): void {
    onValueChange(roundAndClamp(newValue));
  }

  function increment(delta: number): void {
    if (typeof shownNumberRaw !== 'number' && shownValue !== '') return;
    changeValue((shownNumberRaw ?? 0) + delta);
  }

  function getIncrementSize(isShiftKeyPressed: boolean, isAltKeyPressed: boolean): number {
    if (isShiftKeyPressed) {
      return majorStepSize;
    }
    if (isAltKeyPressed) {
      return minorStepSize;
    }
    return stepSize;
  }

  return (
    <ControlGroup className={containerClasses} fill={fill}>
      <InputGroup
        autoComplete="off"
        aria-valuemax={max}
        aria-valuemin={min}
        small={small}
        large={large}
        placeholder={placeholder}
        value={shownValue}
        onChange={e => {
          const valueAsString = (e.target as HTMLInputElement).value;
          setShownValue(valueAsString);

          const shownNumber = shownToNumber(valueAsString);
          if (typeof shownNumber === 'number') {
            changeValue(shownNumber);
          }
          if (valueAsString === '' && onValueEmpty) {
            onValueEmpty();
          }
        }}
        onBlur={e => {
          setShownValue(numberToShown(effectiveValue));
          onBlur?.(e);
        }}
        onKeyDown={e => {
          const { keyCode } = e;

          if (keyCode === Keys.ENTER && typeof shownNumberClamped === 'number') {
            setShownValue(numberToShown(shownNumberClamped));
            return;
          }

          let direction = 0;
          if (keyCode === Keys.ARROW_UP) {
            direction = 1;
          } else if (keyCode === Keys.ARROW_DOWN) {
            direction = -1;
          }

          if (direction) {
            // when the input field has focus, some key combinations will modify
            // the field's selection range. we'll actually want to select all
            // text in the field after we modify the value on the following
            // lines. preventing the default selection behavior lets us do that
            // without interference.
            e.preventDefault();

            increment(direction * getIncrementSize(e.shiftKey, e.altKey));
          }
        }}
      />
      <ButtonGroup className={Classes.FIXED} vertical>
        <Button
          aria-label="increment"
          disabled={effectiveDisabled || isIncrementDisabled}
          icon={IconNames.CHEVRON_UP}
          intent={intent}
          onMouseDown={e => increment(getIncrementSize(e.shiftKey, e.altKey))}
        />
        <Button
          aria-label="decrement"
          disabled={effectiveDisabled || isDecrementDisabled}
          icon={IconNames.CHEVRON_DOWN}
          intent={intent}
          onMouseDown={e => increment(-getIncrementSize(e.shiftKey, e.altKey))}
        />
      </ButtonGroup>
    </ControlGroup>
  );
});
