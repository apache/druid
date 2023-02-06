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

import React from 'react';

import { ExternalLink, Field } from '../../components';
import { getLink } from '../../links';
import { typeIs } from '../../utils';

export interface FlattenSpec {
  useFieldDiscovery?: boolean;
  fields?: FlattenField[];
}

export interface FlattenField {
  name: string;
  type: string;
  expr: string;
}

export const FLATTEN_FIELD_FIELDS: Field<FlattenField>[] = [
  {
    name: 'name',
    type: 'string',
    placeholder: 'column_name',
    required: true,
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['path', 'jq', 'root'],
    required: true,
  },
  {
    name: 'expr',
    type: 'string',
    placeholder: '$.thing',
    defined: typeIs('path', 'jq'),
    required: true,
    info: (
      <>
        Specify a flatten{' '}
        <ExternalLink href={`${getLink('DOCS')}/ingestion/flatten-json`}>expression</ExternalLink>.
      </>
    ),
  },
];

export type ArrayHandling = 'ignore-arrays' | 'include-arrays';

function escapePathKey(pathKey: string): string {
  return /^[a-z]\w*$/i.test(pathKey)
    ? `.${pathKey}`
    : `['${pathKey.replace(/\\/g, '\\\\').replace(/'/g, "\\'")}']`;
}

export function computeFlattenPathsForData(
  data: Record<string, any>[],
  arrayHandling: ArrayHandling,
): FlattenField[] {
  return computeFlattenExprsForData(data, arrayHandling).map(expr => {
    return {
      name: expr.replace(/^\$\./, '').replace(/['\]]/g, '').replace(/\[/g, '.'),
      type: 'path',
      expr,
    };
  });
}

export function computeFlattenExprsForData(
  data: Record<string, any>[],
  arrayHandling: ArrayHandling,
  includeTopLevel = false,
): string[] {
  const seenPaths: Record<string, boolean> = {};
  for (const datum of data) {
    if (!datum || typeof datum !== 'object') continue;
    const datumKeys = Object.keys(datum);
    for (const datumKey of datumKeys) {
      const datumValue = datum[datumKey];
      if (includeTopLevel || isNested(datumValue)) {
        addPath(seenPaths, `$${escapePathKey(datumKey)}`, datumValue, arrayHandling);
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
        addPath(paths, `${path}${escapePathKey(valueKey)}`, value[valueKey], arrayHandling);
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
