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

import { Code } from '@blueprintjs/core';
import React from 'react';

import { ExternalLink, Field } from '../../components';
import { getLink } from '../../links';
import { TIME_COLUMN } from '../timestamp-spec/timestamp-spec';

export interface TransformSpec {
  readonly transforms?: Transform[];
  readonly filter?: Record<string, any>;
}

export interface Transform {
  readonly type: string;
  readonly name: string;
  readonly expression: string;
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

export function getTimestampExpressionFields(transforms: Transform[]): Field<Transform[]>[] {
  const timeTransformIndex = transforms.findIndex(transform => transform.name === '__time');
  if (timeTransformIndex < 0) return [];

  return [
    {
      name: `${timeTransformIndex}.expression`,
      label: 'Expression',
      type: 'string',
      placeholder: `timestamp_parse(concat("date", ' ', "time"))`,
      required: true,
      suggestions: [
        `timestamp_parse(concat("date", ' ', "time"))`,
        `timestamp_parse(concat("date", ' ', "time"), 'M/d/yyyy H:mm:ss')`,
        `timestamp_parse(concat("year", '-', "month", '-', "day"))`,
        `timestamp_parse("west_coast_time", 'yyyy-MM-dd\\'T\\'HH:mm:ss.SSS', 'America/Los_Angeles')`,
        `timestamp_parse("local_time", 'yyyy-MM-dd HH:mm:ss', "timezone")`,
      ],
      info: (
        <>
          A valid Druid{' '}
          <ExternalLink href={`${getLink('DOCS')}/misc/math-expr.html`}>expression</ExternalLink>{' '}
          that should output a millis timestamp. You most likely want to use the{' '}
          <Code>timestamp_parse</Code> function at the outer level.
        </>
      ),
    },
  ];
}

export function addTimestampTransform(transforms: Transform[]): Transform[] {
  return [
    {
      name: TIME_COLUMN,
      type: 'expression',
      expression: '',
    },
  ].concat(transforms);
}

export function removeTimestampTransform(transforms: Transform[]): Transform[] | undefined {
  const newTransforms = transforms.filter(transform => transform.name !== TIME_COLUMN);
  return newTransforms.length ? newTransforms : undefined;
}

export function getDimensionNamesFromTransforms(transforms: Transform[]): string[] {
  return transforms.map(t => t.name).filter(n => n !== TIME_COLUMN);
}
