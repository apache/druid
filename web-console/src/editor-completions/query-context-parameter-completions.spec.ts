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
  defaultValueProperty,
  getQueryContextCompletions,
  queryContextDescriptionToDocMarkdown,
  resolveDocsLink,
} from './query-context-parameter-completions';

describe('query-context-parameter-completions', () => {
  describe('getQueryContextCompletions', () => {
    it('builds the completions of a list of parameters once', () => {
      const queryContextParameters: QueryContextParameterMetadata[] = [
        {
          name: 'useCache',
          type: 'Boolean',
          description: 'Whether to use the cache.',
          default_value: 'true',
          default_description: null,
          effective_value: null,
          effective_source: null,
          deprecation: null,
        },
      ];

      const completions = getQueryContextCompletions(false, queryContextParameters);
      expect(getQueryContextCompletions(false, queryContextParameters)).toBe(completions);
      // A reloaded list gets its own completions
      expect(getQueryContextCompletions(false, [...queryContextParameters])).not.toBe(completions);
    });

    it('notes that the Broker may override the built-in default without CONFIG read access', () => {
      const queryContextParameters: QueryContextParameterMetadata[] = [
        {
          name: 'useCache',
          type: 'Boolean',
          description: 'Whether to use the cache.',
          default_value: 'true',
          default_description: null,
          effective_value: null,
          effective_source: 'UNAUTHORIZED',
          deprecation: null,
        },
      ];

      expect(getQueryContextCompletions(false, queryContextParameters)[0].doc).toEqual({
        name: 'useCache',
        descriptionMarkdown:
          'Type: `Boolean`\n\nDefault: `true` (built-in; the Broker may override it)\n\nWhether to use the cache.',
      });
    });

    it('notes a default query context override and adds no note to a built-in default', () => {
      const parameter: QueryContextParameterMetadata = {
        name: 'useCache',
        type: 'Boolean',
        description: 'Whether to use the cache.',
        default_value: 'true',
        default_description: null,
        effective_value: 'false',
        effective_source: 'DEFAULT_CONTEXT_PROPERTY',
        deprecation: null,
      };

      expect(getQueryContextCompletions(false, [parameter])[0].doc).toEqual({
        name: 'useCache',
        descriptionMarkdown:
          'Type: `Boolean`\n\nDefault: `false` (default query context; built-in: `true`)\n\nWhether to use the cache.',
      });
      expect(
        getQueryContextCompletions(false, [
          { ...parameter, effective_value: 'true', effective_source: 'BUILT_IN' },
        ])[0].doc,
      ).toEqual({
        name: 'useCache',
        descriptionMarkdown: 'Type: `Boolean`\n\nDefault: `true`\n\nWhether to use the cache.',
      });
    });

    it('describes a default that is the value of a runtime property', () => {
      const parameter: QueryContextParameterMetadata = {
        name: 'timeout',
        type: 'Long',
        description: 'Query timeout in milliseconds.',
        default_value: '${druid.server.http.defaultQueryTimeout}',
        default_description: null,
        effective_value: '60000',
        effective_source: 'RUNTIME_PROPERTY',
        deprecation: null,
      };
      const doc = (overrides: Partial<QueryContextParameterMetadata>) =>
        getQueryContextCompletions(false, [{ ...parameter, ...overrides }])[0].doc;

      expect(doc({})).toEqual({
        name: 'timeout',
        descriptionMarkdown:
          'Type: `Long`\n\nDefault: `60000` (runtime property `druid.server.http.defaultQueryTimeout`)\n\nQuery timeout in milliseconds.',
      });
      expect(
        doc({ effective_value: '30000', effective_source: 'DEFAULT_CONTEXT_PROPERTY' }),
      ).toEqual({
        name: 'timeout',
        descriptionMarkdown:
          'Type: `Long`\n\nDefault: `30000` (default query context; runtime property `druid.server.http.defaultQueryTimeout`)\n\nQuery timeout in milliseconds.',
      });
      expect(doc({ effective_value: null, effective_source: 'UNAUTHORIZED' })).toEqual({
        name: 'timeout',
        descriptionMarkdown:
          'Type: `Long`\n\nDefault: value of runtime property `druid.server.http.defaultQueryTimeout` (the Broker may override it)\n\nQuery timeout in milliseconds.',
      });
      expect(doc({ effective_value: null, effective_source: null })).toEqual({
        name: 'timeout',
        descriptionMarkdown:
          'Type: `Long`\n\nDefault: value of runtime property `druid.server.http.defaultQueryTimeout`\n\nQuery timeout in milliseconds.',
      });
    });

    it('quotes a parameter name that needs it unless a quote is being typed', () => {
      const queryContextParameters: QueryContextParameterMetadata[] = [
        {
          name: 'my-key',
          type: 'String',
          description: 'A key that is not a plain identifier.',
          default_value: null,
          default_description: null,
          effective_value: null,
          effective_source: null,
          deprecation: null,
        },
      ];

      expect(getQueryContextCompletions(false, queryContextParameters).map(c => c.label)).toEqual([
        '"my-key"',
      ]);
      expect(getQueryContextCompletions(true, queryContextParameters).map(c => c.label)).toEqual([
        'my-key',
      ]);
    });
  });

  describe('defaultValueProperty', () => {
    it('reads the runtime property of a default_value', () => {
      expect(defaultValueProperty('${druid.query.scan.maxRowsQueuedForOrdering}')).toEqual(
        'druid.query.scan.maxRowsQueuedForOrdering',
      );
      expect(defaultValueProperty('true')).toBeUndefined();
      expect(defaultValueProperty(null)).toBeUndefined();
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

    it('decodes entities once', () => {
      expect(
        queryContextDescriptionToDocMarkdown('Use <code>&amp;lt;</code> for &amp;lt;.'),
      ).toEqual('Use `&lt;` for &lt;.');
    });
  });
});
