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

import { Button, Card, Elevation } from '@blueprintjs/core';
import React, { useCallback, useEffect, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { useStore } from 'zustand';

import { useResizeObserver } from '../../../hooks';
import { highlightStore } from '../highlight-store/highlight-store';

import './highlight-bubble.scss';

export const HighlightBubble = (props: { referenceContainer: HTMLDivElement | null }) => {
  const { referenceContainer } = props;

  const { highlight } = useStore(highlightStore);

  const { left, top } = useResizeObserver(referenceContainer);

  // drop highlight on ESC and save it on ENTER
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        highlight?.onDrop();
      }

      if (event.key === 'Enter') {
        highlight?.onSave?.(highlight);
      }
    };

    document.addEventListener('keydown', handleKeyDown);

    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [highlight]);

  const style = useMemo(() => {
    if (!highlight) return {};
    return { left: left + highlight.x, top: top + highlight.y };
  }, [left, top, highlight]);

  const onSave = useCallback(() => {
    highlight?.onSave?.(highlight);
  }, [highlight]);

  if (!highlight) return null;

  return createPortal(
    <Card elevation={Elevation.TWO} className="highlight-bubble" style={style}>
      <div className="label">{highlight.label}</div>
      <div className="button-group">
        <Button
          intent="none"
          text={highlight.onSave ? 'Cancel' : 'Close'}
          onClick={highlight.onDrop}
        />
        {highlight.onSave && <Button intent="primary" text="Filter" onClick={onSave} />}
      </div>
    </Card>,
    document.body,
  );
};
