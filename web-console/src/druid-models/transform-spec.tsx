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

import { ExternalLink, Field } from '../components';
import { getLink } from '../links';

export interface TransformSpec {
  transforms?: Transform[];
  filter?: Record<string, any>;
}

export interface Transform {
  type: string;
  name: string;
  expression: string;
}

export const TRANSFORM_FIELDS: Field<Transform>[] = [
  {
    name: 'name',
    type: 'string',
    placeholder: 'output_name',
    required: true,
  },
  {
    name: 'type',
    type: 'string',
    suggestions: ['expression'],
    required: true,
  },
  {
    name: 'expression',
    type: 'string',
    placeholder: '"foo" + "bar"',
    required: true,
    info: (
      <>
        A valid Druid{' '}
        <ExternalLink href={`${getLink('DOCS')}/misc/math-expr.html`}>expression</ExternalLink>.
      </>
    ),
  },
];
