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

import { InputGroup, InputGroupProps2 } from '@blueprintjs/core';
import classNames from 'classnames';
import React, { useState } from 'react';

import { Formatter } from '../../utils';

export interface FormattedInputGroupProps extends InputGroupProps2 {
  formatter: Formatter<any>;
  onValueChange: (newValue: undefined | string) => void;
}

export const FormattedInputGroup = React.memo(function FormattedInputGroup(
  props: FormattedInputGroupProps,
) {
  const { className, formatter, value, defaultValue, onValueChange, onBlur, ...rest } = props;

  const [intermediateValue, setIntermediateValue] = useState<string | undefined>();

  return (
    <InputGroup
      className={classNames('formatted-input-group', className)}
      value={
        typeof intermediateValue !== 'undefined'
          ? intermediateValue
          : typeof value !== 'undefined'
          ? formatter.stringify(value)
          : undefined
      }
      defaultValue={
        typeof defaultValue !== 'undefined' ? formatter.stringify(defaultValue) : undefined
      }
      onChange={e => {
        const rawValue = e.target.value;
        setIntermediateValue(rawValue);

        let parsedValue: string | undefined;
        try {
          parsedValue = formatter.parse(rawValue);
        } catch {
          return;
        }
        onValueChange(parsedValue);
      }}
      onBlur={e => {
        setIntermediateValue(undefined);
        onBlur?.(e);
      }}
      {...rest}
    />
  );
});
