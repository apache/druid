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

import type { QueryContextParameterMetadata } from '../druid-models';

import {
  getQueryContextCompletions,
  queryContextDescriptionToDocMarkdown,
  resolveDocsLink,
} from './query-context-parameter-completions';

describe('query-context-parameter-completions', () => {
  describe('getQueryContextCompletions', () => {
    const parameter: QueryContextParameterMetadata = {
      name: 'p',
      type: 'Long',
      description: 'D.',
      default_value: null,
      default_description: null,
      effective_value: null,
      effective_source: null,
      deprecation: null,
      query_languages: ['JSON', 'SQL'],
    };
    const property = '${druid.x}';

    // Only the combinations that sys.query_context_parameters returns
    it.each<[string, Partial<QueryContextParameterMetadata>, string]>([
      [
        'built-in',
        { default_value: '1', effective_value: '1', effective_source: 'BUILT_IN' },
        'Default: `1`',
      ],
      [
        'built-in without CONFIG read access',
        { default_value: '1', effective_source: 'UNAUTHORIZED' },
        'Default: `1` (built-in; the Broker may override it)',
      ],
      [
        'default query context',
        { default_value: '1', effective_value: '2', effective_source: 'DEFAULT_CONTEXT_PROPERTY' },
        'Default: `2` (default query context; built-in: `1`)',
      ],
      [
        'Broker dynamic config',
        { default_value: '1', effective_value: '2', effective_source: 'BROKER_DYNAMIC_CONFIG' },
        'Default: `2` (Broker dynamic config; built-in: `1`)',
      ],
      [
        'runtime property',
        { default_value: property, effective_value: '5', effective_source: 'RUNTIME_PROPERTY' },
        'Default: `5` (runtime property `druid.x`)',
      ],
      [
        'runtime property overridden by the default query context',
        {
          default_value: property,
          effective_value: '6',
          effective_source: 'DEFAULT_CONTEXT_PROPERTY',
        },
        'Default: `6` (default query context; runtime property `druid.x`)',
      ],
      [
        'runtime property that is not set',
        { default_value: property },
        'Default: value of runtime property `druid.x`',
      ],
      [
        'runtime property without CONFIG read access',
        { default_value: property, effective_source: 'UNAUTHORIZED' },
        'Default: value of runtime property `druid.x` (the Broker may override it)',
      ],
      ['described', { default_description: 'auto' }, 'Default: auto'],
      [
        'described without CONFIG read access',
        { default_description: 'auto', effective_source: 'UNAUTHORIZED' },
        'Default: auto (the Broker may override it)',
      ],
    ])('shows a default that is %s', (_, overrides, expected) => {
      expect(getQueryContextCompletions(false, [{ ...parameter, ...overrides }])[0].doc).toEqual({
        name: 'p',
        descriptionMarkdown: `Type: \`Long\`\n\n${expected}\n\nD.`,
      });
    });

    it('sorts a deprecated parameter after the others and notes the deprecation', () => {
      const [completion] = getQueryContextCompletions(false, [
        { ...parameter, deprecation: 'Use `q` instead.' },
      ]);
      expect(completion.boost).toEqual(40);
      expect(completion.doc?.descriptionMarkdown).toEqual(
        'Type: `Long`\n\nD.\n\n*Deprecated*: Use `q` instead.',
      );
    });

    it('does not suggest parameters that SQL can not provide', () => {
      expect(
        getQueryContextCompletions(false, [
          { ...parameter, name: 'useCache' },
          { ...parameter, name: 'bySegment', query_languages: ['JSON'] },
        ]).map(c => c.label),
      ).toEqual(['useCache']);
    });

    it('quotes a parameter name that needs it unless a quote is being typed', () => {
      const queryContextParameters = [{ ...parameter, name: 'my-key' }];
      expect(getQueryContextCompletions(false, queryContextParameters).map(c => c.label)).toEqual([
        '"my-key"',
      ]);
      expect(getQueryContextCompletions(true, queryContextParameters).map(c => c.label)).toEqual([
        'my-key',
      ]);
    });
  });

  describe('resolveDocsLink', () => {
    it('resolves links relative to the querying docs to the docs site', () => {
      const docs = 'https://druid.apache.org/docs/latest';
      expect(resolveDocsLink('dart.md')).toEqual(`${docs}/querying/dart`);
      expect(resolveDocsLink('./filters.md#null-filter')).toEqual(
        `${docs}/querying/filters#null-filter`,
      );
      expect(resolveDocsLink('../multi-stage-query/reference.md#joins')).toEqual(
        `${docs}/multi-stage-query/reference#joins`,
      );
      expect(resolveDocsLink('../configuration/index.md#broker')).toEqual(
        `${docs}/configuration/#broker`,
      );
      // Anchors as the docs site makes them
      expect(
        resolveDocsLink('../configuration/index.md#Guardrails for materialization of subqueries'),
      ).toEqual(`${docs}/configuration/#guardrails-for-materialization-of-subqueries`);
      expect(resolveDocsLink('https://example.com/x')).toEqual('https://example.com/x');
    });

    it('does not resolve links to an anchor of the same page or to other schemes', () => {
      expect(resolveDocsLink('#limits')).toBeUndefined();
      expect(resolveDocsLink('javascript:alert(1)')).toBeUndefined();
      expect(resolveDocsLink('http://example.com/x')).toBeUndefined();
    });
  });

  describe('queryContextDescriptionToDocMarkdown', () => {
    it('turns links into docs links and HTML into line breaks and list items', () => {
      expect(
        queryContextDescriptionToDocMarkdown(
          'Uses [lookups](lookups.md#reverse) for `IN`, see [limits](#limits).<br /><br />Options:<ul><li>`a`: first</li><li>`b`: second</li></ul>Done.',
        ),
      ).toEqual(
        'Uses [lookups](https://druid.apache.org/docs/latest/querying/lookups#reverse) for `IN`, see limits.\n\nOptions:\n- `a`: first\n- `b`: second\nDone.',
      );
    });

    it('converts links whose text is a code span and leaves brackets in code spans alone', () => {
      expect(
        queryContextDescriptionToDocMarkdown(
          'An [`indexSpec`](../ingestion/ingestion-spec.md#indexspec) for `ARRAY[1](2)`, see [`this`](#section).',
        ),
      ).toEqual(
        'An [`indexSpec`](https://druid.apache.org/docs/latest/ingestion/ingestion-spec#indexspec) for `ARRAY[1](2)`, see `this`.',
      );
    });

    it('keeps code spans as they are and converts code tags, strong text and entities', () => {
      expect(
        queryContextDescriptionToDocMarkdown(
          '**Deprecated.** Threads are named `processing_<queryId>`; use <code>&#124;&#124;</code> &amp; more.',
        ),
      ).toEqual('*Deprecated.* Threads are named `processing_<queryId>`; use `||` & more.');
    });

    it('drops tags that removing another tag leaves behind', () => {
      expect(queryContextDescriptionToDocMarkdown('a <<b>i>tag</i> b')).toEqual('a tag b');
    });

    it('decodes hex entities and leaves entities that are not code points as they are', () => {
      expect(queryContextDescriptionToDocMarkdown('a &#x7C; b &#99999999; c &#0;')).toEqual(
        'a | b &#99999999; c &#0;',
      );
    });

    it('decodes entities once', () => {
      expect(
        queryContextDescriptionToDocMarkdown('Use <code>&amp;lt;</code> for &amp;lt;.'),
      ).toEqual('Use `&lt;` for &lt;.');
    });
  });
});
