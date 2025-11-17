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

import { NATIVE_JSON_QUERY_COMPLETIONS } from '../druid-models';

import type { GetHjsonCompletionsOptions } from './hjson-completions';
import { getHjsonCompletions } from './hjson-completions';

describe('getHjsonCompletions', () => {
  const baseOptions: GetHjsonCompletionsOptions = {
    jsonCompletions: NATIVE_JSON_QUERY_COMPLETIONS,
    textBefore: '',
    charBeforePrefix: '',
    prefix: '',
  };

  it('returns empty array for comments', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": "scan", // This is a comment with ',
      prefix: 'S',
    });
    expect(completions).toEqual([]);
  });

  it('returns root level property suggestions', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('queryType');
    expect(completionValues).toContain('dataSource');
  });

  it('returns queryType value suggestions', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": ',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('timeseries');
    expect(completionValues).toContain('topN');
    expect(completionValues).toContain('groupBy');
    expect(completionValues).toContain('scan');
  });

  it('returns query-specific properties based on queryType', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": "timeseries", ',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('granularity');
    expect(completionValues).toContain('aggregations');
    expect(completionValues).toContain('intervals');

    // Should not suggest properties that already exist
    expect(completionValues).not.toContain('queryType');
  });

  it('returns granularity value suggestions', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": "timeseries", "granularity": ',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('hour');
    expect(completionValues).toContain('day');
    expect(completionValues).toContain('all');
  });

  it('returns aggregation properties in array context', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": "timeseries", "aggregations": [{ ',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('type');
    expect(completionValues).toContain('name');
  });

  it('returns aggregation type values', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": "timeseries", "aggregations": [{ "type": ',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('count');
    expect(completionValues).toContain('longSum');
    expect(completionValues).toContain('doubleSum');
  });

  it('returns fieldName for field-based aggregators', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "queryType": "timeseries", "aggregations": [{ "type": "longSum", ',
    });

    const completionValues = completions.map(c => c.value);
    expect(completionValues).toContain('fieldName');
    expect(completionValues).toContain('name');

    // Should not suggest properties that already exist
    expect(completionValues).not.toContain('type');
  });

  it('handles quoted vs unquoted completions', () => {
    const unquotedCompletions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ ',
      charBeforePrefix: '',
    });

    const quotedCompletions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{ "',
      charBeforePrefix: '"',
    });

    // Unquoted should include quotes for property names
    const unquotedValues = unquotedCompletions.map(c => c.value);
    expect(unquotedValues.some(v => v.includes('"'))).toBe(false); // Simple property names don't need quotes

    // Quoted completions should not add extra quotes
    const quotedValues = quotedCompletions.map(c => c.value);
    expect(quotedValues).toContain('queryType');
    expect(quotedValues).toContain('dataSource');
  });

  it('provides documentation for completions', () => {
    const completions = getHjsonCompletions({
      ...baseOptions,
      textBefore: '{',
    });

    const queryTypeCompletion = completions.find(c => c.value === 'queryType');
    expect(queryTypeCompletion).toBeDefined();
    expect(queryTypeCompletion?.meta).toBe('property');
  });
});
