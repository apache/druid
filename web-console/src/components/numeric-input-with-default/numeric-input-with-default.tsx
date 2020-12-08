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

import { HTMLInputProps, INumericInputProps, NumericInput } from '@blueprintjs/core';
import React, { useState } from 'react';

export type NumericInputWithDefaultProps = HTMLInputProps & INumericInputProps;

export const NumericInputWithDefault = React.memo(function NumericInputWithDefault(
  props: NumericInputWithDefaultProps,
) {
  const { value, defaultValue, onValueChange, onBlur, ...rest } = props;
  const [hasChanged, setHasChanged] = useState(false);

  let effectiveValue = value;
  if (effectiveValue == null) {
    effectiveValue = hasChanged ? '' : typeof defaultValue !== 'undefined' ? defaultValue : '';
  }

  return (
    <NumericInput
      value={effectiveValue}
      onValueChange={(valueAsNumber, valueAsString, inputElement) => {
        setHasChanged(true);
        if (!onValueChange) return;
        return onValueChange(valueAsNumber, valueAsString, inputElement);
      }}
      onBlur={e => {
        setHasChanged(false);
        if (!onBlur) return;
        return onBlur(e);
      }}
      {...rest}
    />
  );
});
