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

import { TextArea } from '@blueprintjs/core';
import React from 'react';

export interface ArrayInputProps {
  className?: string;
  values: string[];
  onChange: (newValues: string[]) => void;
  placeholder?: string;
  large?: boolean;
  disabled?: boolean;
}

export class ArrayInput extends React.PureComponent<ArrayInputProps, { stringValue: string }> {
  constructor(props: ArrayInputProps) {
    super(props);
    this.state = {
      stringValue: Array.isArray(props.values) ? props.values.join(', ') : '',
    };
  }

  private handleChange = (e: any) => {
    const { onChange } = this.props;
    const stringValue = e.target.value;
    const newValues = stringValue.split(',').map((v: string) => v.trim());
    const newValuesFiltered = newValues.filter(Boolean);
    this.setState({
      stringValue:
        newValues.length === newValuesFiltered.length ? newValues.join(', ') : stringValue,
    });
    if (onChange) onChange(stringValue === '' ? undefined : newValuesFiltered);
  };

  render() {
    const { className, placeholder, large, disabled } = this.props;
    const { stringValue } = this.state;
    return (
      <TextArea
        className={className}
        value={stringValue}
        onChange={this.handleChange}
        placeholder={placeholder}
        large={large}
        disabled={disabled}
        fill
      />
    );
  }
}
