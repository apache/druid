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

import { Classes, Icon, Menu, MenuItem, Popover, Position, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';

import type { OptionValue } from '../../models';

export interface OptionsInputProps {
  options: readonly OptionValue[];
  value: OptionValue[];
  onValueChange(value: OptionValue[]): void;
  optionLabel?(o: OptionValue): string;
  allowDuplicates?: boolean;
  nonEmpty?: boolean;
}

export const OptionsInput = function OptionsInput(props: OptionsInputProps) {
  const { options, value, onValueChange, optionLabel = String, allowDuplicates, nonEmpty } = props;

  const selectedOptions: OptionValue[] = value.filter(v => options.includes(v));

  const availableOptions = allowDuplicates
    ? options
    : options.filter(o => !value.find(v => v === o));

  const canRemove = !nonEmpty || options.length > 1;
  return (
    <div className={classNames('options-input', Classes.INPUT, Classes.TAG_INPUT, Classes.FILL)}>
      <div className={Classes.TAG_INPUT_VALUES}>
        {selectedOptions.map((selectedOption, i) => (
          <Popover
            key={i}
            position={Position.BOTTOM}
            content={
              <Menu>
                {(allowDuplicates
                  ? options
                  : options.filter(o => o === selectedOption || !value.find(v => v === o))
                ).map((ao, j) => (
                  <MenuItem
                    key={j}
                    text={optionLabel(ao)}
                    labelElement={
                      ao === selectedOption ? <Icon icon={IconNames.TICK} /> : undefined
                    }
                    onClick={() => {
                      onValueChange(value.map(v => (v === selectedOption ? ao : v)));
                    }}
                  />
                ))}
              </Menu>
            }
          >
            <Tag
              interactive
              onRemove={
                canRemove
                  ? () => {
                      onValueChange(value.filter(v => v !== selectedOption));
                    }
                  : undefined
              }
            >
              {optionLabel(selectedOption)}
            </Tag>
          </Popover>
        ))}
        <Popover
          position={Position.BOTTOM}
          content={
            <Menu>
              {availableOptions.map((ao, i) => (
                <MenuItem
                  key={i}
                  text={optionLabel(ao)}
                  onClick={() => {
                    onValueChange(value.concat(ao));
                  }}
                />
              ))}
            </Menu>
          }
        >
          <Tag icon={IconNames.PLUS} interactive />
        </Popover>
      </div>
    </div>
  );
};
