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

import { MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

export interface MenuCheckboxProps {
  text: string;
  checked: boolean;
  onChange: () => void;
}

export function MenuCheckbox(props: MenuCheckboxProps) {
  const { text, checked, onChange } = props;

  return (
    <MenuItem
      className="menu-checkbox"
      icon={checked ? IconNames.TICK_CIRCLE : IconNames.CIRCLE}
      text={text}
      onClick={onChange}
      shouldDismissPopover={false}
    />
  );
}
