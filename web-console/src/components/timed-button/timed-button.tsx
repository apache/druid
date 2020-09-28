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

import { Button, ButtonGroup, IButtonProps, Popover, Radio, RadioGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { useInterval } from '../../hooks';
import { localStorageGet, LocalStorageKeys, localStorageSet } from '../../utils';

import './timed-button.scss';

export interface DelayLabel {
  label: string;
  delay: number;
}

export interface TimedButtonProps extends IButtonProps {
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

  const [delay, setDelay] = useState(
    localStorageKey && localStorageGet(localStorageKey)
      ? Number(localStorageGet(localStorageKey))
      : defaultDelay,
  );

  useInterval(() => {
    onRefresh(true);
  }, delay);

  function handleSelection(e: any) {
    const selectedDelay = Number(e.currentTarget.value);
    setDelay(selectedDelay);
    if (localStorageKey) {
      localStorageSet(localStorageKey, String(selectedDelay));
    }
  }

  return (
    <ButtonGroup>
      <Button {...other} text={text} icon={icon} onClick={() => onRefresh(false)} />
      <Popover
        content={
          <RadioGroup
            label={label}
            className="timed-button"
            onChange={handleSelection}
            selectedValue={delay}
          >
            {delays.map(({ label, delay }) => (
              <Radio label={label} value={delay} key={label} />
            ))}
          </RadioGroup>
        }
      >
        <Button {...other} rightIcon={IconNames.CARET_DOWN} />
      </Popover>
    </ButtonGroup>
  );
});
