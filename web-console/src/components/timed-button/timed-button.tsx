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

import { Button, ButtonGroup, ButtonProps, Menu, MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

import { useInterval } from '../../hooks';
import { localStorageGet, LocalStorageKeys, localStorageSet } from '../../utils';

export interface DelayLabel {
  label: string;
  delay: number;
}

export interface TimedButtonProps extends ButtonProps {
  delays: DelayLabel[];
  onRefresh: (auto: boolean) => void;
  localStorageKey?: LocalStorageKeys;
  label: string;
  defaultDelay: number;
}

export const TimedButton = React.memo(function TimedButton(props: TimedButtonProps) {
  const {
    label,
    delays,
    onRefresh,
    type,
    text,
    icon,
    defaultDelay,
    localStorageKey,
    ...other
  } = props;

  const [selectedDelay, setSelectedDelay] = useState(
    localStorageKey && localStorageGet(localStorageKey)
      ? Number(localStorageGet(localStorageKey))
      : defaultDelay,
  );

  useInterval(() => {
    onRefresh(true);
  }, selectedDelay);

  function handleSelection(delay: number) {
    setSelectedDelay(delay);
    if (localStorageKey) {
      localStorageSet(localStorageKey, String(delay));
    }
  }

  return (
    <ButtonGroup className="timed-button">
      <Button {...other} text={text} icon={icon} onClick={() => onRefresh(false)} />
      <Popover2
        content={
          <Menu>
            <MenuDivider title={label} />
            {delays.map(({ label, delay }, i) => (
              <MenuItem
                key={i}
                icon={selectedDelay === delay ? IconNames.SELECTION : IconNames.CIRCLE}
                text={label}
                onClick={() => handleSelection(delay)}
              />
            ))}
          </Menu>
        }
      >
        <Button {...other} rightIcon={IconNames.CARET_DOWN} />
      </Popover2>
    </ButtonGroup>
  );
});
