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

import { useCallback, useEffect, useState } from 'react';

const emptyDOMRect = { height: 0, width: 0, x: 0, y: 0, bottom: 0, left: 0, right: 0, top: 0 };

function createEmptyDOMRect(): DOMRect {
  // DOMRect is not defined in JSDOM
  return typeof DOMRect !== 'undefined'
    ? new DOMRect()
    : { ...emptyDOMRect, toJSON: () => emptyDOMRect };
}

export function useResizeObserver(element: HTMLElement | null | undefined) {
  const [rect, setRect] = useState(() => createEmptyDOMRect());

  const maybeUpdate = useCallback(
    (e: Element) => {
      const newRect: DOMRect = e.getBoundingClientRect();
      if (
        !rect ||
        rect.bottom !== newRect.bottom ||
        rect.top !== newRect.top ||
        rect.left !== newRect.left ||
        rect.right !== newRect.right ||
        rect.width !== newRect.width ||
        rect.height !== newRect.height
      ) {
        setRect(newRect);
      }
    },
    [rect],
  );

  const onScroll = useCallback(() => {
    if (element) {
      maybeUpdate(element);
    }
  }, [element, maybeUpdate]);

  useEffect(() => {
    const resizeObserver = new ResizeObserver((entries: ResizeObserverEntry[]) => {
      if (!Array.isArray(entries) || !entries.length) {
        return;
      }

      maybeUpdate(entries[0].target);
    });

    if (element) {
      resizeObserver.observe(element);
      window.addEventListener('scroll', onScroll, true);

      return () => {
        resizeObserver.unobserve(element);
        window.removeEventListener('scroll', onScroll, true);
      };
    }

    return;
  }, [element, maybeUpdate, onScroll]);

  return rect;
}
