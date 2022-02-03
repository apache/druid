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

import { InputGroup, InputGroupProps2, Intent, TextArea } from '@blueprintjs/core';
import { Tooltip2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React, { useState } from 'react';

import { Formatter } from '../../utils';

import './formatted-input.scss';

export interface FormattedInputProps extends InputGroupProps2 {
  formatter: Formatter<any>;
  onValueChange: (newValue: undefined | string) => void;
  sanitizer?: (rawValue: string) => string;
  issueWithValue?: (value: any) => string | undefined;
  multiline?: boolean;
}

export const FormattedInput = React.memo(function FormattedInput(props: FormattedInputProps) {
  const {
    className,
    formatter,
    sanitizer,
    issueWithValue,
    value,
    defaultValue,
    onValueChange,
    onFocus,
    onBlur,
    intent,
    placeholder,
    multiline,
    ...rest
  } = props;

  const [intermediateValue, setIntermediateValue] = useState<string | undefined>();
  const [isFocused, setIsFocused] = useState(false);

  const issue: string | undefined = issueWithValue?.(value);
  const showIssue = Boolean(!isFocused && issue);

  const myValue =
    typeof intermediateValue !== 'undefined'
      ? intermediateValue
      : typeof value !== 'undefined'
      ? formatter.stringify(value)
      : undefined;

  const myDefaultValue =
    typeof defaultValue !== 'undefined' ? formatter.stringify(defaultValue) : undefined;

  const myOnChange = (e: any) => {
    let rawValue = e.target.value;
    if (sanitizer) rawValue = sanitizer(rawValue);
    setIntermediateValue(rawValue);

    let parsedValue: string | undefined;
    try {
      parsedValue = formatter.parse(rawValue);
    } catch {
      return;
    }
    onValueChange(parsedValue);
  };

  const myOnFocus = (e: any) => {
    setIsFocused(true);
    onFocus?.(e);
  };

  const myOnBlur = (e: any) => {
    setIntermediateValue(undefined);
    setIsFocused(false);
    onBlur?.(e);
  };

  const myIntent = showIssue ? Intent.DANGER : intent;

  return (
    <div className={classNames('formatted-input', className)}>
      {multiline ? (
        <TextArea
          value={myValue}
          defaultValue={myDefaultValue}
          onChange={myOnChange}
          onFocus={myOnFocus}
          onBlur={myOnBlur}
          intent={myIntent}
          placeholder={placeholder}
        />
      ) : (
        <InputGroup
          value={myValue}
          defaultValue={myDefaultValue}
          onChange={myOnChange}
          onFocus={myOnFocus}
          onBlur={myOnBlur}
          intent={myIntent}
          placeholder={placeholder}
          {...rest}
        />
      )}
      {showIssue && (
        <Tooltip2
          isOpen
          content={showIssue ? issue : undefined}
          position="right"
          intent={Intent.DANGER}
          targetTagName="div"
        >
          <div className="target-dummy" />
        </Tooltip2>
      )}
    </div>
  );
});
