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

import { FlattenField } from './ingestion-spec';

export type ExprType = 'path' | 'jq';
export type ArrayHandling = 'ignore-arrays' | 'include-arrays';

export function computeFlattenPathsForData(
  data: Record<string, any>[],
  exprType: ExprType,
  arrayHandling: ArrayHandling,
): FlattenField[] {
  return computeFlattenExprsForData(data, exprType, arrayHandling).map(expr => {
    return {
      name: expr.replace(/^\$?\./, ''),
      type: exprType,
      expr,
    };
  });
}

export function computeFlattenExprsForData(
  data: Record<string, any>[],
  exprType: ExprType,
  arrayHandling: ArrayHandling,
): string[] {
  const seenPaths: Record<string, boolean> = {};
  for (const datum of data) {
    const datumKeys = Object.keys(datum);
    for (const datumKey of datumKeys) {
      const datumValue = datum[datumKey];
      if (isNested(datumValue)) {
        addPath(
          seenPaths,
          exprType === 'path' ? `$.${datumKey}` : `.${datumKey}`,
          datumValue,
          arrayHandling,
        );
      }
    }
  }

  return Object.keys(seenPaths).sort();
}

function addPath(
  paths: Record<string, boolean>,
  path: string,
  value: any,
  arrayHandling: ArrayHandling,
) {
  if (isNested(value)) {
    if (!Array.isArray(value)) {
      const valueKeys = Object.keys(value);
      for (const valueKey of valueKeys) {
        addPath(paths, `${path}.${valueKey}`, value[valueKey], arrayHandling);
      }
    } else if (arrayHandling === 'include-arrays') {
      for (let i = 0; i < value.length; i++) {
        addPath(paths, `${path}[${i}]`, value[i], arrayHandling);
      }
    }
  } else {
    paths[path] = true;
  }
}

// Checks that the given value is nested as far as Druid is concerned
function isNested(v: any): boolean {
  return Boolean(v) && typeof v === 'object' && (!Array.isArray(v) || v.some(isNested));
}
