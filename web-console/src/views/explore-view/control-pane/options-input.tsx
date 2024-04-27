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

import { Classes, Menu, MenuItem, Position, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { OptionValue, ParameterDefinition } from '@druid-toolkit/visuals-core';
import { getPluginOptionLabel } from '@druid-toolkit/visuals-core';
import classNames from 'classnames';
import React from 'react';

export interface OptionsInputProps {
  parameter: ParameterDefinition;
  options: readonly OptionValue[];
  value: OptionValue[];
  onValueChange(value: OptionValue[]): void;
}

export const OptionsInput = function OptionsInput(props: OptionsInputProps) {
  const { options, value, onValueChange, parameter } = props;

  if (parameter.type !== 'options') {
    return null;
  }

  const selectedOptions: OptionValue[] = value.filter(v => options.includes(v));

  const availableOptions = parameter.allowDuplicates
    ? options
    : options.filter(o => !value.find(v => v === o));

  return (
    <div className={classNames('options-input', Classes.INPUT, Classes.TAG_INPUT, Classes.FILL)}>
      <div className={Classes.TAG_INPUT_VALUES}>
        {selectedOptions.map((o, i) => (
          <Tag
            interactive
            key={i}
            onRemove={() => {
              onValueChange(value.filter(v => v !== o));
            }}
          >
            {getPluginOptionLabel(o, parameter)}
          </Tag>
        ))}
        <Popover2
          position={Position.BOTTOM}
          content={
            <Menu>
              {availableOptions.map((o, i) => (
                <MenuItem
                  key={i}
                  text={getPluginOptionLabel(o, parameter)}
                  onClick={() => {
                    onValueChange(value.concat(o));
                  }}
                />
              ))}
            </Menu>
          }
        >
          <Tag icon={IconNames.PLUS} interactive />
        </Popover2>
      </div>
    </div>
  );
};
