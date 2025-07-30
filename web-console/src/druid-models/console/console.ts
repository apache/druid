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

import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';

export type ConsoleViewId =
  | 'data-loader'
  | 'streaming-data-loader'
  | 'classic-batch-data-loader'
  | 'supervisors'
  | 'tasks'
  | 'datasources'
  | 'segments'
  | 'services'
  | 'workbench'
  | 'sql-data-loader'
  | 'explore'
  | 'lookups';

export function getConsoleViewIcon(view: ConsoleViewId): IconName {
  switch (view) {
    case 'workbench':
      return IconNames.APPLICATION;
    case 'data-loader':
      return IconNames.CLOUD_UPLOAD;
    case 'streaming-data-loader':
      return IconNames.FEED;
    case 'sql-data-loader':
      return IconNames.CLEAN;
    case 'classic-batch-data-loader':
      return IconNames.LIST;
    case 'datasources':
      return IconNames.MULTI_SELECT;
    case 'supervisors':
      return IconNames.EYE_OPEN;
    case 'tasks':
      return IconNames.GANTT_CHART;
    case 'segments':
      return IconNames.STACKED_CHART;
    case 'services':
      return IconNames.DATABASE;
    case 'lookups':
      return IconNames.PROPERTIES;
    case 'explore':
      return IconNames.MAP;
    default:
      return IconNames.HELP;
  }
}
