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

import { Button, Menu, MenuItem, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React, { useRef } from 'react';

import { JSON_STRING_FORMATTER } from '../../utils';
import {
  FormattedInputGroup,
  FormattedInputGroupProps,
} from '../formatted-input-group/formatted-input-group';

export interface SuggestionGroup {
  group: string;
  suggestions: string[];
}

export type Suggestion = undefined | string | SuggestionGroup;

export interface SuggestibleInputProps extends Omit<FormattedInputGroupProps, 'formatter'> {
  onFinalize?: () => void;
  suggestions?: Suggestion[];
}

export const SuggestibleInput = React.memo(function SuggestibleInput(props: SuggestibleInputProps) {
  const {
    className,
    value,
    onValueChange,
    onFinalize,
    onBlur,
    onFocus,
    suggestions,
    ...rest
  } = props;

  const lastFocusValue = useRef<string>();

  function handleSuggestionSelect(suggestion: undefined | string) {
    onValueChange(suggestion);
    if (onFinalize) onFinalize();
  }

  return (
    <FormattedInputGroup
      className={classNames('suggestible-input', className)}
      formatter={JSON_STRING_FORMATTER}
      value={value}
      onValueChange={onValueChange}
      onFocus={e => {
        lastFocusValue.current = e.target.value;
        onFocus?.(e);
      }}
      onBlur={e => {
        onBlur?.(e);
        if (lastFocusValue.current === e.target.value) return;
        onFinalize?.();
      }}
      rightElement={
        suggestions && (
          <Popover2
            content={
              <Menu>
                {suggestions.map(suggestion => {
                  if (typeof suggestion === 'undefined') {
                    return (
                      <MenuItem
                        key="__undefined__"
                        text="(none)"
                        onClick={() => handleSuggestionSelect(suggestion)}
                      />
                    );
                  } else if (typeof suggestion === 'string') {
                    return (
                      <MenuItem
                        key={suggestion}
                        text={JSON_STRING_FORMATTER.stringify(suggestion)}
                        onClick={() => handleSuggestionSelect(suggestion)}
                      />
                    );
                  } else {
                    return (
                      <MenuItem key={suggestion.group} text={suggestion.group}>
                        {suggestion.suggestions.map(suggestion => (
                          <MenuItem
                            key={suggestion}
                            text={suggestion}
                            onClick={() => handleSuggestionSelect(suggestion)}
                          />
                        ))}
                      </MenuItem>
                    );
                  }
                })}
              </Menu>
            }
            position={Position.BOTTOM_RIGHT}
            autoFocus={false}
          >
            <Button icon={IconNames.CARET_DOWN} minimal />
          </Popover2>
        )
      }
      {...rest}
    />
  );
});
