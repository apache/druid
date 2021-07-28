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

import { Intent, TextArea } from '@blueprintjs/core';
import React, { useState } from 'react';

import { compact } from '../../utils';

export interface ArrayInputProps {
  className?: string;
  values: string[] | undefined;
  onChange: (newValues: string[] | undefined) => void;
  placeholder?: string;
  large?: boolean;
  disabled?: boolean;
  intent?: Intent;
}

export const ArrayInput = React.memo(function ArrayInput(props: ArrayInputProps) {
  const { className, placeholder, large, disabled, intent, onChange } = props;
  const [intermediateValue, setIntermediateValue] = useState<string | undefined>();

  const handleChange = (e: any) => {
    const stringValue = e.target.value;
    setIntermediateValue(stringValue);

    onChange(
      stringValue === ''
        ? undefined
        : compact(stringValue.split(/[,\s]+/).map((v: string) => v.trim())),
    );
  };

  return (
    <TextArea
      className={className}
      value={
        typeof intermediateValue !== 'undefined'
          ? intermediateValue
          : props.values?.join(', ') || ''
      }
      onChange={handleChange}
      onBlur={() => setIntermediateValue(undefined)}
      placeholder={placeholder}
      large={large}
      disabled={disabled}
      intent={intent}
      fill
    />
  );
});
