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

import { clamp } from '../general';

import './mouse-tooltip.scss';

const OFFSET_X = 0;
const OFFSET_Y = 30;

function findDataTooltip(element: HTMLElement): string | undefined {
  let e: HTMLElement | null = element;
  while (e) {
    const t = e.getAttribute('data-tooltip');
    if (t) return t;
    e = e.parentElement;
  }
  return;
}

export function initMouseTooltip() {
  let tooltipDiv: HTMLDivElement | undefined;

  // Mouse move handler to follow the mouse
  const handleMouseMove = (event: MouseEvent) => {
    if (!tooltipDiv) return;

    tooltipDiv.style.left = `${clamp(
      event.pageX + OFFSET_X,
      0,
      window.innerWidth - tooltipDiv.offsetWidth,
    )}px`;
    tooltipDiv.style.top = `${clamp(
      event.pageY + OFFSET_Y,
      0,
      window.innerHeight - tooltipDiv.offsetHeight,
    )}px`;
  };

  // Mouse over handler to show the tooltip for elements with the appropriate attribute
  const handleMouseOver = (event: MouseEvent) => {
    const title = findDataTooltip(event.target as HTMLElement);
    if (title) {
      if (!tooltipDiv) {
        tooltipDiv = document.createElement('div');
        tooltipDiv.className = 'mouse-tooltip';
        document.body.appendChild(tooltipDiv);
        document.addEventListener('mousemove', handleMouseMove);
      }
      if (tooltipDiv.innerText !== title) {
        tooltipDiv.innerText = title;
        handleMouseMove(event); // Position the div as it might be the first positioning
      }
    } else if (tooltipDiv) {
      document.removeEventListener('mousemove', handleMouseMove);
      tooltipDiv.remove();
      tooltipDiv = undefined;
    }
  };

  document.addEventListener('mouseover', handleMouseOver);
}
