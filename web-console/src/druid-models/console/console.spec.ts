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

import { IconNames } from '@blueprintjs/icons';

import { getConsoleViewIcon } from './console';

describe('console', () => {
  describe('getConsoleViewIcon', () => {
    it('returns correct icons for all console views', () => {
      expect(getConsoleViewIcon('workbench')).toBe(IconNames.APPLICATION);
      expect(getConsoleViewIcon('data-loader')).toBe(IconNames.CLOUD_UPLOAD);
      expect(getConsoleViewIcon('streaming-data-loader')).toBe(IconNames.FEED);
      expect(getConsoleViewIcon('sql-data-loader')).toBe(IconNames.CLEAN);
      expect(getConsoleViewIcon('classic-batch-data-loader')).toBe(IconNames.LIST);
      expect(getConsoleViewIcon('datasources')).toBe(IconNames.MULTI_SELECT);
      expect(getConsoleViewIcon('supervisors')).toBe(IconNames.EYE_OPEN);
      expect(getConsoleViewIcon('tasks')).toBe(IconNames.GANTT_CHART);
      expect(getConsoleViewIcon('segments')).toBe(IconNames.STACKED_CHART);
      expect(getConsoleViewIcon('services')).toBe(IconNames.DATABASE);
      expect(getConsoleViewIcon('lookups')).toBe(IconNames.PROPERTIES);
      expect(getConsoleViewIcon('explore')).toBe(IconNames.MAP);
    });

    it('returns HELP icon for unknown views', () => {
      // @ts-expect-error - testing invalid input
      expect(getConsoleViewIcon('unknown-view')).toBe(IconNames.HELP);
    });
  });
});
