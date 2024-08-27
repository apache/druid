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
import React from 'react';

import { tickIcon } from '../../utils';

export interface MenuBooleanProps extends Omit<MenuItemProps, 'label'> {
  value: boolean | undefined;
  onValueChange(value: boolean | undefined): void;
  undefinedLabel?: string;
  undefinedEffectiveValue?: boolean;
  optionsLabelElement?: Partial<Record<'undefined' | 'true' | 'false', ReactNode>>;
}

export function MenuBoolean(props: MenuBooleanProps) {
  const {
    value,
    onValueChange,
    undefinedLabel,
    undefinedEffectiveValue,
    className,
    shouldDismissPopover,
    optionsLabelElement = {},
    ...rest
  } = props;
  const effectiveValue = undefinedLabel ? value : value ?? undefinedEffectiveValue;
  const shouldDismiss = shouldDismissPopover ?? false;

  function formatValue(value: boolean | undefined): string {
    return String(value ?? undefinedLabel ?? 'auto');
  }

  return (
    <MenuItem
      className={classNames('menu-tristate', className)}
      shouldDismissPopover={shouldDismiss}
      label={`${formatValue(effectiveValue)}${
        typeof effectiveValue === 'undefined' && typeof undefinedEffectiveValue === 'boolean'
          ? ` (${undefinedEffectiveValue})`
          : ''
      }`}
      {...rest}
    >
      {(undefinedLabel ? [undefined, true, false] : [true, false]).map(v => (
        <MenuItem
          key={String(v)}
          icon={tickIcon(effectiveValue === v)}
          text={formatValue(v)}
          labelElement={optionsLabelElement[String(v) as 'undefined' | 'true' | 'false']}
          onClick={() => onValueChange(v)}
          shouldDismissPopover={shouldDismiss}
        />
      ))}
    </MenuItem>
  );
}
