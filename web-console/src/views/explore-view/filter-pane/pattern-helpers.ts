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

import type { FilterPattern } from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';

import { DATE_FORMAT } from '../utils';

export function initPatternForColumn(column: ExpressionMeta): FilterPattern {
  switch (column.sqlType) {
    case 'TIMESTAMP':
      return {
        type: 'timeRelative',
        negated: false,
        column: column.name,
        anchor: 'maxDataTime',
        rangeDuration: 'P1D',
      };

    default:
      return {
        type: 'values',
        negated: false,
        column: column.name,
        values: [],
      };
  }
}

export function formatPatternWithoutNegation(pattern: FilterPattern): string {
  switch (pattern.type) {
    case 'values':
      return `${pattern.column}: ${pattern.values
        .map(v => (v === '' ? 'empty' : String(v)))
        .join(', ')}`;

    case 'contains':
      return `${pattern.column} ~ '${pattern.contains}'`;

    case 'regexp':
      return `${pattern.column} ~ /${pattern.regexp}/`;

    case 'timeInterval': {
      return DATE_FORMAT.formatRange(pattern.start, pattern.end);
    }

    case 'timeRelative':
      return `${pattern.column} in ${pattern.rangeDuration}`;

    case 'numberRange':
      return `${pattern.column} in ${pattern.startBound}${pattern.start}, ${pattern.end}${pattern.endBound}`;

    case 'mvContains':
      return `${pattern.column} on of ${pattern.values
        .map(v => (v === '' ? 'empty' : String(v)))
        .join(', ')}`;

    case 'custom':
      return String(pattern.expression);
  }
}
