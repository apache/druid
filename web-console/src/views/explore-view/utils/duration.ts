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

import { sum } from 'd3-array';

import { filterMap, pluralIfNeeded } from '../../../utils';

const SPANS = ['year', 'month', 'day', 'hour', 'minute', 'second'];

export function formatDuration(duration: string, preferCompact = false): string {
  // Regular expressions to match ISO 8601 duration parts
  const regex = /P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?/;
  const matches = duration.match(regex);
  if (!matches) return duration;

  // Extract the relevant parts
  const counts = SPANS.map((_, i) => parseInt(matches[i + 1] || '0', 10));

  // Construct the human-readable format based on the parsed values
  let parts: string[];
  if (preferCompact && sum(counts) === 1) {
    parts = filterMap(SPANS, (span, i) => (counts[i] ? span : undefined));
  } else {
    parts = filterMap(SPANS, (span, i) =>
      counts[i] ? pluralIfNeeded(counts[i], span) : undefined,
    );
  }

  // Join the parts with commas, and return the result
  return parts.length > 0 ? parts.join(', ') : '0 seconds';
}
