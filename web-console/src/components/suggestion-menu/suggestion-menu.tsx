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

import { Menu, MenuItem } from '@blueprintjs/core';
import React from 'react';

import { JSON_STRING_FORMATTER } from '../../utils';

export interface SuggestionGroup {
  group: string;
  suggestions: string[];
}

export type Suggestion = undefined | string | SuggestionGroup;

export interface SuggestionMenuProps {
  suggestions: Suggestion[];
  onSuggest(suggestion: string | undefined): void;
}

export const SuggestionMenu = React.memo(function SuggestionMenu(props: SuggestionMenuProps) {
  const { suggestions, onSuggest } = props;
  return (
    <Menu className="suggestion-menu">
      {suggestions.map(suggestion => {
        if (typeof suggestion === 'undefined') {
          return (
            <MenuItem key="__undefined__" text="(none)" onClick={() => onSuggest(suggestion)} />
          );
        } else if (typeof suggestion === 'string') {
          return (
            <MenuItem
              key={suggestion}
              text={JSON_STRING_FORMATTER.stringify(suggestion)}
              onClick={() => onSuggest(suggestion)}
            />
          );
        } else {
          return (
            <MenuItem key={suggestion.group} text={suggestion.group}>
              {suggestion.suggestions.map(suggestion => (
                <MenuItem
                  key={suggestion}
                  text={suggestion}
                  onClick={() => onSuggest(suggestion)}
                />
              ))}
            </MenuItem>
          );
        }
      })}
    </Menu>
  );
});
