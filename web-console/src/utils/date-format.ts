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

import type { Duration, Timezone } from 'chronoshift';

import { prettyFormatIsoDate, toIsoStringInTimezone } from './date';

export function formatStartDuration(start: Date, duration: Duration): string {
  let sliceLength;
  const { singleSpan } = duration;
  switch (singleSpan) {
    case 'year':
      sliceLength = 4;
      break;

    case 'month':
      sliceLength = 7;
      break;

    case 'day':
      sliceLength = 10;
      break;

    case 'hour':
      sliceLength = 13;
      break;

    case 'minute':
      sliceLength = 16;
      break;

    default:
      sliceLength = 19;
      break;
  }

  return `${start.toISOString().slice(0, sliceLength).replace('T', ' ')}/${duration.toString(
    true,
  )}`;
}

export function formatIsoDateRange(start: Date, end: Date, timezone: Timezone): string {
  let startStr = prettyFormatIsoDate(toIsoStringInTimezone(start, timezone));
  let endStr = prettyFormatIsoDate(toIsoStringInTimezone(end, timezone));

  if (start.getMinutes() === 0 && end.getMinutes() === 0) {
    startStr = startStr.slice(0, 16);
    endStr = endStr.slice(0, 16);
  }

  const startDate = startStr.slice(0, 10);
  if (startDate === endStr.slice(0, 10)) {
    return `${startDate}, ${startStr.slice(11)} → ${endStr.slice(11)}`;
  } else {
    return `${startStr} → ${endStr}`;
  }
}
