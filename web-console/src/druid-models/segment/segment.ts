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

import { Duration } from '../../utils';

export const START_OF_TIME_DATE = '-146136543-09-08T08:23:32.096Z';
export const END_OF_TIME_DATE = '146140482-04-24T15:36:27.903Z';

export function computeSegmentTimeSpan(start: string, end: string): string {
  if (start === START_OF_TIME_DATE && end === END_OF_TIME_DATE) {
    return 'All';
  }

  const startDate = new Date(start);
  if (isNaN(startDate.valueOf())) {
    return 'Invalid start';
  }

  const endDate = new Date(end);
  if (isNaN(endDate.valueOf())) {
    return 'Invalid end';
  }

  return Duration.fromRange(startDate, endDate, 'Etc/UTC').getDescription(true);
}

export interface ShardSpec {
  type: string;
  partitionNum?: number;
  partitions?: number;
  dimensions?: string[];
  partitionDimensions?: string[];
  start?: string[];
  end?: string[];
}
