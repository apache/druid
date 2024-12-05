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

import { Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import type { ReactNode } from 'react';
import { useState } from 'react';
import { createPortal } from 'react-dom';

import { clamp } from '../../utils';

import './portal-bubble.scss';

const SHPITZ_SIZE = 10;

export interface PortalBubbleOpenOn {
  x: number;
  y: number;
  title?: string;
  text: ReactNode;
}

export interface PortalBubbleProps {
  className?: string;
  openOn: PortalBubbleOpenOn | undefined;
  direction?: 'up' | 'down';
  onClose?(): void;
  mute?: boolean;
  minimal?: boolean;
}

export const PortalBubble = function PortalBubble(props: PortalBubbleProps) {
  const { className, openOn, direction = 'up', onClose, mute, minimal } = props;
  const [myWidth, setMyWidth] = useState(200);
  if (!openOn) return null;

  const halfMyWidth = myWidth / 2;

  const x = clamp(openOn.x, halfMyWidth, window.innerWidth - halfMyWidth);
  const offset = clamp(x - openOn.x, -halfMyWidth, halfMyWidth);

  return createPortal(
    <div
      className={classNames('portal-bubble', className, direction, {
        mute: mute && !onClose,
      })}
      ref={element => {
        if (!element) return;
        setMyWidth(element.offsetWidth);
      }}
      style={{
        left: x,
        top: openOn.y + (minimal ? 0 : direction === 'up' ? -SHPITZ_SIZE : SHPITZ_SIZE),
      }}
    >
      {(openOn.title || onClose) && (
        <div className={classNames('bubble-title-bar', { 'with-close': Boolean(onClose) })}>
          {openOn.title}
          {onClose && (
            <Button
              className="close-button"
              icon={IconNames.CROSS}
              small
              minimal
              onClick={onClose}
            />
          )}
        </div>
      )}
      <div className="bubble-content">{openOn.text}</div>
      {!minimal && (
        <div className="shpitz" style={{ left: offset ? `calc(50% - ${offset}px)` : '50%' }} />
      )}
    </div>,
    document.body,
  );
};
