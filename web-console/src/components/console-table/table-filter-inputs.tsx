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
  HTMLSelect,
  Icon,
  InputGroup,
  Menu,
  MenuItem,
  PopoverNext,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import { useEffect, useState } from 'react';

import { filterMap, toggle } from '../../utils';
import { TableFilter } from '../../utils/table-filters';

import type { ConsoleTableFilterProps } from './console-table';

export function GenericFilterInput({ column, filter, onChange }: ConsoleTableFilterProps) {
  const INPUT_DEBOUNCE_TIME_IN_MILLISECONDS = 1000;
  const [menuOpen, setMenuOpen] = useState(false);
  const [focusedText, setFocusedText] = useState<string | undefined>();
  const [debouncedValue, setDebouncedValue] = useState<string | undefined>();

  const enableComparisons = String(column.headerClassName).includes('enable-comparisons');

  const { mode, needle } = (filter ? TableFilter.parseModeAndNeedle(filter, true) : undefined) || {
    mode: '~',
    needle: '',
  };

  useEffect(() => {
    const handler = setTimeout(() => {
      if (focusedText !== undefined && focusedText !== debouncedValue) {
        onChange(TableFilter.combineModeAndNeedle(mode, focusedText));
        setDebouncedValue(focusedText);
      }
    }, INPUT_DEBOUNCE_TIME_IN_MILLISECONDS);

    return () => {
      clearTimeout(handler);
    };
  }, [focusedText, debouncedValue, mode, onChange]);

  return (
    <InputGroup
      className={classNames('generic-filter-input', {
        'hide-icon': !filter && !(menuOpen || typeof focusedText === 'string'),
      })}
      leftElement={
        <PopoverNext
          placement="bottom-start"
          animation="minimal"
          arrow={false}
          isOpen={menuOpen}
          onInteraction={setMenuOpen}
          content={
            <Menu>
              {(enableComparisons ? TableFilter.MODES : TableFilter.MODES_NO_COMPARISON).map(
                (m, i) => (
                  <MenuItem
                    key={i}
                    icon={TableFilter.modeToIcon(m)}
                    text={TableFilter.modeToTitle(m)}
                    onClick={() => onChange(TableFilter.combineModeAndNeedle(m, needle))}
                    labelElement={m === mode ? <Icon icon={IconNames.TICK} /> : undefined}
                  />
                ),
              )}
            </Menu>
          }
          lazy
          shouldReturnFocusOnClose={false}
        >
          <Button
            className="filter-mode-button"
            icon={TableFilter.modeToIcon(mode)}
            variant="minimal"
          />
        </PopoverNext>
      }
      value={focusedText ?? needle}
      onChange={e => setFocusedText(e.target.value.trimStart())}
      onKeyDown={e => {
        if (e.key === 'Enter') {
          const inputValue = (e.target as HTMLInputElement).value;
          setDebouncedValue(undefined); // Reset debounce to avoid duplicate triggers
          onChange(TableFilter.combineModeAndNeedle(mode, inputValue));
        }
      }}
      rightElement={
        filter ? (
          <Button icon={IconNames.CROSS} variant="minimal" onClick={() => onChange('')} />
        ) : undefined
      }
      onBlur={e => {
        setFocusedText(undefined);
        if (filter && !e.target.value) onChange('');
      }}
    />
  );
}

export function suggestibleFilterInput(suggestions: string[]) {
  return function SuggestibleFilterInput(props: ConsoleTableFilterProps) {
    const { filter, onChange } = props;
    let valuesFilteredOn: string[] | undefined;
    if (filter) {
      const modeAndNeedle = TableFilter.parseModeAndNeedle(filter, true);
      if (modeAndNeedle && modeAndNeedle.mode === '=') {
        valuesFilteredOn = modeAndNeedle.needleParts;
      }
    }
    return (
      <PopoverNext
        placement="bottom-start"
        animation="minimal"
        arrow={false}
        content={
          <Menu>
            {filterMap(suggestions, (suggestion, i) => {
              return (
                <MenuItem
                  key={i}
                  icon={
                    valuesFilteredOn
                      ? valuesFilteredOn.includes(suggestion)
                        ? IconNames.MINUS
                        : IconNames.PLUS
                      : IconNames.EQUALS
                  }
                  text={suggestion}
                  onClick={() =>
                    onChange(
                      TableFilter.combineModeAndNeedle(
                        '=',
                        valuesFilteredOn
                          ? TableFilter.joinNeedle(toggle(valuesFilteredOn, suggestion))
                          : TableFilter.joinNeedle([suggestion]),
                        true,
                      ),
                    )
                  }
                />
              );
            })}
          </Menu>
        }
        lazy
        shouldReturnFocusOnClose={false}
      >
        <GenericFilterInput {...props} />
      </PopoverNext>
    );
  };
}

export function BooleanFilterInput({ filter, onChange }: ConsoleTableFilterProps) {
  return (
    <HTMLSelect
      className="boolean-filter-input"
      style={{ width: '100%' }}
      onChange={(event: any) => onChange(event.target.value)}
      value={String(filter?.value ?? '')}
      fill
    >
      <option value="">Show all</option>
      <option value="=true">true</option>
      <option value="=false">false</option>
    </HTMLSelect>
  );
}
