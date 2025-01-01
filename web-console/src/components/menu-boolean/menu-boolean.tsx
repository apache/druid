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

import type { MenuItemProps } from '@blueprintjs/core';
import { MenuItem } from '@blueprintjs/core';
import classNames from 'classnames';
import type { ReactNode } from 'react';

import { tickIcon } from '../../utils';

export type TrueFalseUndefined = 'true' | 'false' | 'undefined';

function toKey(value: boolean | undefined) {
  return String(value) as TrueFalseUndefined;
}

const DEFAULT_OPTIONS_TEXT: Partial<Record<TrueFalseUndefined, string>> = { undefined: 'Auto' };

export const ENABLE_DISABLE_OPTIONS_TEXT: Partial<Record<TrueFalseUndefined, string>> = {
  true: 'Enable',
  false: 'Disable',
  undefined: 'Auto',
};

export interface MenuBooleanProps extends Omit<MenuItemProps, 'label'> {
  value: boolean | undefined;
  onValueChange(value: boolean | undefined): void;
  showUndefined?: boolean;
  undefinedEffectiveValue?: boolean;
  optionsText?: Partial<Record<TrueFalseUndefined, string>>;
  optionsLabelElement?: Partial<Record<TrueFalseUndefined, ReactNode>>;
}

export function MenuBoolean(props: MenuBooleanProps) {
  const {
    value,
    onValueChange,
    showUndefined,
    undefinedEffectiveValue,
    className,
    shouldDismissPopover,
    optionsText = DEFAULT_OPTIONS_TEXT,
    optionsLabelElement = {},
    ...rest
  } = props;
  const effectiveValue = showUndefined ? value : value ?? undefinedEffectiveValue;
  const shouldDismiss = shouldDismissPopover ?? false;

  function formatValue(value: boolean | undefined): string {
    const s = toKey(value);
    return optionsText[s] ?? s;
  }

  return (
    <MenuItem
      className={classNames('menu-tristate', className)}
      shouldDismissPopover={shouldDismiss}
      label={`${formatValue(effectiveValue)}${
        typeof effectiveValue === 'undefined' && typeof undefinedEffectiveValue === 'boolean'
          ? ` (${formatValue(undefinedEffectiveValue)})`
          : ''
      }`}
      {...rest}
    >
      {(showUndefined ? [undefined, true, false] : [true, false]).map(v => (
        <MenuItem
          key={String(v)}
          icon={tickIcon(effectiveValue === v)}
          text={formatValue(v)}
          labelElement={optionsLabelElement[toKey(v)]}
          onClick={() => onValueChange(v)}
          shouldDismissPopover={shouldDismiss}
        />
      ))}
    </MenuItem>
  );
}
