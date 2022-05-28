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

import { Button, Menu, MenuItem, PopoverPosition } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import React from 'react';

import './live-query-mode-selector.scss';

export type LiveQueryMode = 'auto' | 'on' | 'off';
export const LIVE_QUERY_MODES: LiveQueryMode[] = ['auto', 'on', 'off'];
export const LIVE_QUERY_MODE_TITLE: Record<LiveQueryMode, string> = {
  auto: 'Auto',
  on: 'On',
  off: 'Off',
};

export interface LiveQueryModeSelectorProps {
  liveQueryMode: LiveQueryMode;
  onLiveQueryModeChange: (liveQueryMode: LiveQueryMode) => void;
  autoLiveQueryModeShouldRun: boolean;
}

export const LiveQueryModeSelector = React.memo(function LiveQueryModeSelector(
  props: LiveQueryModeSelectorProps,
) {
  const { liveQueryMode, onLiveQueryModeChange, autoLiveQueryModeShouldRun } = props;

  return (
    <Popover2
      portalClassName="live-query-mode-selector-portal"
      minimal
      position={PopoverPosition.BOTTOM_LEFT}
      content={
        <Menu>
          {LIVE_QUERY_MODES.map(m => (
            <MenuItem
              className={classNames(
                m,
                m === 'auto' ? (autoLiveQueryModeShouldRun ? 'auto-on' : 'auto-off') : undefined,
              )}
              key={m}
              icon={m === liveQueryMode ? IconNames.TICK : IconNames.BLANK}
              text={LIVE_QUERY_MODE_TITLE[m]}
              onClick={() => onLiveQueryModeChange(m)}
            />
          ))}
        </Menu>
      }
    >
      <Button minimal className="live-query-mode-selector">
        <span>Live query:</span>{' '}
        <span
          className={classNames(liveQueryMode, autoLiveQueryModeShouldRun ? 'auto-on' : 'auto-off')}
        >
          {LIVE_QUERY_MODE_TITLE[liveQueryMode]}
        </span>
      </Button>
    </Popover2>
  );
});
