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

import { EditorState } from '@codemirror/state';
import { sane } from 'druid-query-toolkit';

import { hjson } from '../editor-languages/hjson';

import { getHjsonContext } from './hjson-context';

// The context with the cursor at the |
function contextAt(textWithCursor: string) {
  const pos = textWithCursor.indexOf('|');
  const state = EditorState.create({
    doc: textWithCursor.slice(0, pos) + textWithCursor.slice(pos + 1),
    extensions: hjson(),
  });
  return getHjsonContext(state, pos);
}

describe('getHjsonContext', () => {
  describe('root level', () => {
    it('detects cursor at empty object', () => {
      expect(contextAt('{|')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
      expect(contextAt('{ |')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
      expect(contextAt('{|}')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });

    it('detects cursor at empty array', () => {
      expect(contextAt('[|')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: '0',
        currentObject: {},
      });
    });

    it('detects a key at the root without braces', () => {
      expect(contextAt('que|')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
      expect(contextAt('queryType: scan\nda|')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { queryType: 'scan' },
      });
      expect(contextAt('queryType: scan\ndataSource: |')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'dataSource',
        currentObject: { queryType: 'scan' },
      });
    });
  });

  describe('key editing', () => {
    it('detects cursor while typing a key', () => {
      expect(contextAt('{ que|')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });

    it('detects cursor while typing a quoted key', () => {
      expect(contextAt('{ "que|')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });

    it('detects cursor after comma expecting new key', () => {
      expect(contextAt('{ "queryType": "scan", |')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { queryType: 'scan' },
      });
    });

    it('detects cursor in the middle of a key', () => {
      expect(contextAt('{ "que|ryType": "scan" }')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });
  });

  describe('value editing', () => {
    it('detects cursor after colon expecting value', () => {
      expect(contextAt('{ "queryType": |')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        currentObject: {},
      });
    });

    it('detects cursor while typing a string value', () => {
      expect(contextAt('{ "queryType": "sc|')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        currentObject: {},
      });
    });

    it('detects cursor in unquoted value (Hjson feature)', () => {
      expect(contextAt('{ queryType: sc|')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        currentObject: {},
      });
    });

    it('detects cursor right after a value', () => {
      expect(contextAt('{ "queryType": "scan"|')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        currentObject: {},
      });
      expect(contextAt('{ "filter": {}|')).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'filter',
        currentObject: {},
      });
    });
  });

  describe('nested objects', () => {
    it('detects cursor in nested object key position', () => {
      expect(contextAt('{ "query": { |')).toEqual({
        path: ['query'],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });

    it('detects cursor in deeply nested value position', () => {
      expect(contextAt('{ "query": { "dataSource": { "type": |')).toEqual({
        path: ['query', 'dataSource'],
        isEditingKey: false,
        currentKey: 'type',
        currentObject: {},
      });
    });

    it('detects cursor in nested object after comma', () => {
      expect(contextAt('{ "query": { "dataSource": "wikipedia", "queryType": |')).toEqual({
        path: ['query'],
        isEditingKey: false,
        currentKey: 'queryType',
        currentObject: { dataSource: 'wikipedia' },
      });
    });
  });

  describe('arrays', () => {
    it('detects cursor in array first position', () => {
      expect(contextAt('{ "dimensions": [|')).toEqual({
        path: ['dimensions'],
        isEditingKey: false,
        currentKey: '0',
        currentObject: {},
      });
    });

    it('detects cursor in array after first element', () => {
      expect(contextAt('{ "dimensions": ["page", |')).toEqual({
        path: ['dimensions'],
        isEditingKey: false,
        currentKey: '1',
        currentObject: {},
      });
    });

    it('detects cursor in nested object within array', () => {
      expect(contextAt('{ "filters": [{ "type": |')).toEqual({
        path: ['filters', '0'],
        isEditingKey: false,
        currentKey: 'type',
        currentObject: {},
      });
    });

    it('detects cursor in array element object key position', () => {
      expect(contextAt('{ "filters": [{ "type": "selector", |')).toEqual({
        path: ['filters', '0'],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { type: 'selector' },
      });
    });

    it('detects cursor in a later object within array', () => {
      expect(contextAt('{ "filters": [{ "type": "selector" }, { "type": "in", |')).toEqual({
        path: ['filters', '1'],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { type: 'in' },
      });
    });

    it('gives the object that the array is in', () => {
      expect(contextAt('{ "queryType": "scan", "columns": [|], "limit": 10 }')).toEqual({
        path: ['columns'],
        isEditingKey: false,
        currentKey: '0',
        currentObject: { queryType: 'scan', limit: 10 },
      });
    });
  });

  describe('the text after the cursor', () => {
    it('includes the properties after the cursor', () => {
      expect(
        contextAt(sane`
          {
            |
            "queryType": "topN",
            "threshold": 10
          }
        `),
      ).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { queryType: 'topN', threshold: 10 },
      });
    });

    it('detects a value that is being typed before the next property', () => {
      expect(
        contextAt(sane`
          {
            "queryType": "scan",
            "dataSource": |
            "limit": 10
          }
        `),
      ).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'dataSource',
        currentObject: { queryType: 'scan' },
      });
    });
  });

  describe('complex scenarios', () => {
    it('handles multiline Hjson', () => {
      expect(
        contextAt(`{
  "queryType": "groupBy",
  "dataSource": "wikipedia",
  "dimensions": [
    {
      "type": "default",
      "dimension": |`),
      ).toEqual({
        path: ['dimensions', '0'],
        isEditingKey: false,
        currentKey: 'dimension',
        currentObject: { type: 'default' },
      });
    });

    it('handles Hjson comments and multiline strings', () => {
      expect(
        contextAt(sane`
          {
            // This is a comment
            "queryType": "scan",
            # This is also a comment
            interval:
              '''
              Hello
              World
              '''
            /* Multi-line
               comment */
            "dataSource": |
        `),
      ).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'dataSource',
        currentObject: { queryType: 'scan', interval: 'Hello\nWorld' },
      });
    });

    it('handles keys after a comma or a new line (Hjson feature)', () => {
      expect(
        contextAt(sane`
          {
            "queryType": "scan",
            "dataSource": "wikipedia",
            m|
        `),
      ).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { queryType: 'scan', dataSource: 'wikipedia' },
      });

      expect(
        contextAt(sane`
          {
            "queryType": "scan",
            "dataSource": "wikipedia"
            m|
        `),
      ).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: { queryType: 'scan', dataSource: 'wikipedia' },
      });
    });

    it('handles quoteless keys and values', () => {
      expect(
        contextAt(sane`
          {
            queryType: topN
            intervals: ...
            dataSource: sdsds
            filter: {
              t|
        `),
      ).toEqual({
        path: ['filter'],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });

    it('handles incomplete nested structure', () => {
      expect(
        contextAt(sane`
          {
            "queryType": "scan",
            "dataSource": {
              "type": "restrict",
              "base": {
                "type": "table",
                "name": "wikipedia"
              },
              "policy": {
                "type": "noRestriction"
              }
            },
            "intervals": {
              "type": "intervals",
              "intervals": [
                "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
              ]
            },
            |
        `),
      ).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {
          queryType: 'scan',
          dataSource: {
            type: 'restrict',
            base: { type: 'table', name: 'wikipedia' },
            policy: { type: 'noRestriction' },
          },
          intervals: {
            type: 'intervals',
            intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
          },
        },
      });
    });

    it('reads the values of the other properties', () => {
      expect(
        contextAt(
          '{ "s": "a\\"b", "n": -1.5e3, "t": true, "f": false, "z": null, "q": hello world\n  |',
        ).currentObject,
      ).toEqual({ s: 'a"b', n: -1500, t: true, f: false, z: null, q: 'hello world' });
    });
  });

  describe('edge cases', () => {
    it('handles empty string', () => {
      expect(contextAt('|')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });

    it('handles just whitespace', () => {
      expect(contextAt('   |')).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        currentObject: {},
      });
    });
  });
});
