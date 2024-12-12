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

import { nonEmptyArray } from '../../../utils';
import type { ParameterDefinition } from '../models';

export function toggle<T>(xs: readonly T[], x: T, eq?: (a: T, b: T) => boolean): T[] {
  const e = eq || ((a, b) => a === b);
  return xs.find(_ => e(_, x)) ? xs.filter(d => !e(d, x)) : xs.concat([x]);
}

export function normalizeType(paramType: ParameterDefinition['type']): ParameterDefinition['type'] {
  switch (paramType) {
    case 'expressions':
      return 'expression';

    case 'measures':
      return 'measure';

    default:
      return paramType;
  }
}

export function adjustTransferValue(
  value: unknown,
  sourceType: ParameterDefinition['type'],
  targetType: ParameterDefinition['type'],
) {
  const comboType: `${ParameterDefinition['type']}->${ParameterDefinition['type']}` = `${sourceType}->${targetType}`;
  switch (comboType) {
    case 'measure->measures':
    case 'expression->expressions':
      return [value];

    case 'measures->measure':
    case 'expressions->expression':
      return nonEmptyArray(value) ? value[0] : undefined;

    default:
      return value;
  }
}
