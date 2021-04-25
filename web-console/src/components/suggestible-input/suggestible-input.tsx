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

import {
  Button,
  HTMLInputProps,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import React, { useRef } from 'react';

export interface SuggestionGroup {
  group: string;
  suggestions: string[];
}

export type Suggestion = undefined | string | SuggestionGroup;

export interface SuggestibleInputProps extends HTMLInputProps {
  onValueChange: (newValue: undefined | string) => void;
  onFinalize?: () => void;
  suggestions?: Suggestion[];
  large?: boolean;
  intent?: Intent;
}

export const SuggestibleInput = React.memo(function SuggestibleInput(props: SuggestibleInputProps) {
  const {
    className,
    value,
    defaultValue,
    onValueChange,
    onFinalize,
    onBlur,
    suggestions,
    ...rest
  } = props;

  const lastFocusValue = useRef<string>();

  function handleSuggestionSelect(suggestion: undefined | string) {
    onValueChange(suggestion);
    if (onFinalize) onFinalize();
  }

  return (
    <InputGroup
      className={classNames('suggestible-input', className)}
      value={value as string}
      defaultValue={defaultValue as string}
      onChange={(e: any) => {
        onValueChange(e.target.value);
      }}
      onFocus={(e: any) => {
        lastFocusValue.current = e.target.value;
      }}
      onBlur={(e: any) => {
        if (onBlur) onBlur(e);
        if (lastFocusValue.current === e.target.value) return;
        if (onFinalize) onFinalize();
      }}
      rightElement={
        suggestions && (
          <Popover
            boundary="window"
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
                        text={suggestion}
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
          </Popover>
        )
      }
      {...rest}
    />
  );
});
