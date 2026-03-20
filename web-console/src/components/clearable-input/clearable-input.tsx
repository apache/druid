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

import type { InputGroupProps } from '@blueprintjs/core';
import { Button, InputGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import React from 'react';

export interface ClearableInputProps extends InputGroupProps {
  className?: string;
  value: string;
  onValueChange: (value: string) => void;
}

export const ClearableInput = React.memo(function ClearableInput(props: ClearableInputProps) {
  const { className, value, onValueChange, ...otherProps } = props;

  return (
    <InputGroup
      className={classNames('clearable-input', className)}
      value={value}
      onChange={(e: any) => onValueChange(e.target.value.trimStart())}
      rightElement={
        value ? (
          <Button icon={IconNames.CROSS} minimal onClick={() => onValueChange('')} />
        ) : undefined
      }
      {...otherProps}
    />
  );
});
