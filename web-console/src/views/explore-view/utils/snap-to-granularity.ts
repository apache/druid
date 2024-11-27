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

import { Duration, Timezone } from 'chronoshift';

/**
 * Will try to snap start and end to the closest available dates, given the granularity.
 *
 * @param start the start date
 * @param end the end date
 * @param granularity the granularity
 * @param timezone the timezone
 * @returns an object with the start and end dates snapped to the given granularity
 */
export function snapToGranularity(
  start: Date,
  end: Date,
  granularity: string,
  timezone?: string,
): { start: Date; end: Date } {
  const tz = Timezone.fromJS(timezone || 'Etc/UTC');
  const duration = Duration.fromJS(granularity);

  // get closest to start
  const flooredStart = duration.floor(start, tz);
  const ceiledStart = duration.shift(flooredStart, tz, 1);
  const distanceToFlooredStart = Math.abs(start.valueOf() - flooredStart.valueOf());
  const distanceToCeiledStart = Math.abs(start.valueOf() - ceiledStart.valueOf());
  const closestStart = distanceToFlooredStart < distanceToCeiledStart ? flooredStart : ceiledStart;

  // get closest to end
  const flooredEnd = duration.floor(end, tz);
  const ceiledEnd = duration.shift(flooredEnd, tz, 1);
  const distanceToFlooredEnd = Math.abs(end.valueOf() - flooredEnd.valueOf());
  const distanceToCeiledEnd = Math.abs(end.valueOf() - ceiledEnd.valueOf());
  const closestEnd = distanceToFlooredEnd < distanceToCeiledEnd ? flooredEnd : ceiledEnd;

  return {
    start: closestStart,
    end: closestEnd,
  };
}
