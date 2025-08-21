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
import { sane } from 'druid-query-toolkit';

import { getHjsonContext } from './hjson-context';

describe('getHjsonContext', () => {
  describe('root level', () => {
    it('detects cursor at empty object', () => {
      const result = getHjsonContext('{');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor after opening brace with whitespace', () => {
      const result = getHjsonContext('{ ');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor at empty array treated as object', () => {
      const result = getHjsonContext('[');
      expect(result).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: '0',
        isEditingComment: false,
        currentObject: {},
      });
    });
  });

  describe('key editing', () => {
    it('detects cursor while typing a key', () => {
      const result = getHjsonContext('{ que');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor while typing a quoted key', () => {
      const result = getHjsonContext('{ "que');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor after comma expecting new key', () => {
      const result = getHjsonContext('{ "queryType": "scan", ');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {
          queryType: 'scan',
        },
      });
    });
  });

  describe('value editing', () => {
    it('detects cursor after colon expecting value', () => {
      const result = getHjsonContext('{ "queryType": ');
      expect(result).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor while typing a string value', () => {
      const result = getHjsonContext('{ "queryType": "sc');
      expect(result).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor in unquoted value (Hjson feature)', () => {
      const result = getHjsonContext('{ queryType: sc');
      expect(result).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        isEditingComment: false,
        currentObject: {},
      });
    });
  });

  describe('nested objects', () => {
    it('detects cursor in nested object key position', () => {
      const result = getHjsonContext('{ "query": { ');
      expect(result).toEqual({
        path: ['query'],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor in deeply nested value position', () => {
      const result = getHjsonContext('{ "query": { "dataSource": { "type": ');
      expect(result).toEqual({
        path: ['query', 'dataSource'],
        isEditingKey: false,
        currentKey: 'type',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor in nested object after comma', () => {
      const result = getHjsonContext('{ "query": { "dataSource": "wikipedia", "queryType": ');
      expect(result).toEqual({
        path: ['query'],
        isEditingKey: false,
        currentKey: 'queryType',
        isEditingComment: false,
        currentObject: { dataSource: 'wikipedia' },
      });
    });
  });

  describe('arrays as objects', () => {
    it('detects cursor in array first position', () => {
      const result = getHjsonContext('{ "dimensions": [');
      expect(result).toEqual({
        path: ['dimensions'],
        isEditingKey: false,
        currentKey: '0',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor in array after first element', () => {
      const result = getHjsonContext('{ "dimensions": ["page", ');
      expect(result).toEqual({
        path: ['dimensions'],
        isEditingKey: false,
        currentKey: '1',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor in nested object within array', () => {
      const result = getHjsonContext('{ "filters": [{ "type": ');
      expect(result).toEqual({
        path: ['filters', '0'],
        isEditingKey: false,
        currentKey: 'type',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('detects cursor in array element object key position', () => {
      const result = getHjsonContext('{ "filters": [{ "type": "selector", ');
      expect(result).toEqual({
        path: ['filters', '0'],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: { type: 'selector' },
      });
    });
  });

  describe('complex scenarios', () => {
    it('handles multiline Hjson', () => {
      const hjson = `{
  "queryType": "groupBy",
  "dataSource": "wikipedia",
  "dimensions": [
    {
      "type": "default",
      "dimension": `;
      const result = getHjsonContext(hjson);
      expect(result).toEqual({
        path: ['dimensions', '0'],
        isEditingKey: false,
        currentKey: 'dimension',
        isEditingComment: false,
        currentObject: { type: 'default' },
      });
    });

    it('handles Hjson comments', () => {
      const result = getHjsonContext(sane`
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
          "dataSource":
      `);
      expect(result).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'dataSource',
        isEditingComment: false,
        currentObject: { queryType: 'scan', interval: 'Hello\nWorld' },
      });
    });

    it('handles trailing commas (Hjson feature)', () => {
      const result = getHjsonContext(sane`
        {
          "queryType": "scan",
          "dataSource": "wikipedia",
          m
      `);
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: 'm',
        isEditingComment: false,
        currentObject: { queryType: 'scan', dataSource: 'wikipedia' },
      });
    });

    it('handles no trailing commas (Hjson feature)', () => {
      const result = getHjsonContext(sane`
        {
          "queryType": "scan",
          "dataSource": "wikipedia"
          m
      `);
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: 'm',
        isEditingComment: false,
        currentObject: { queryType: 'scan', dataSource: 'wikipedia' },
      });
    });

    it('custom', () => {
      const result = getHjsonContext(
        sane`
          {
            queryType: "topN"
            intervals: "..."
            dataSource: "sdsds"
            filter: {
              t`,
      );
      expect(result).toEqual({
        currentObject: {},
        isEditingComment: false,
        isEditingKey: true,
        path: ['filter'],
        currentKey: 't',
      });
    });

    it('handles incomplete nested structure', () => {
      const result = getHjsonContext(sane`
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
      `);
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {
          queryType: 'scan',
          dataSource: {
            type: 'restrict',
            base: {
              type: 'table',
              name: 'wikipedia',
            },
            policy: {
              type: 'noRestriction',
            },
          },
          intervals: {
            type: 'intervals',
            intervals: ['-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z'],
          },
        },
      });
    });
  });

  describe('edge cases', () => {
    it('handles empty string', () => {
      const result = getHjsonContext('');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('handles just whitespace', () => {
      const result = getHjsonContext('   ');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('handles unclosed string', () => {
      const result = getHjsonContext('{ "queryType": "scan');
      expect(result).toEqual({
        path: [],
        isEditingKey: false,
        currentKey: 'queryType',
        isEditingComment: false,
        currentObject: {},
      });
    });

    it('handles cursor in middle of key', () => {
      // | represents cursor position conceptually
      // Assuming we only get text up to cursor, so:
      const actualInput = '{ "query';
      const actualResult = getHjsonContext(actualInput);
      expect(actualResult).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: false,
        currentObject: {},
      });
    });
  });

  describe('comment detection', () => {
    it('detects cursor inside single-line comment', () => {
      const result = getHjsonContext('{ "queryType": "scan", // This is a com');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: true,
        currentObject: { queryType: 'scan' },
      });
    });

    it('detects cursor inside multi-line comment', () => {
      const result = getHjsonContext('{ "queryType": "scan", /* This is a multi-line\n   com');
      expect(result).toEqual({
        path: [],
        isEditingKey: true,
        currentKey: undefined,
        isEditingComment: true,
        currentObject: { queryType: 'scan' },
      });
    });
  });

  describe('currentObject property', () => {
    it('returns empty object placeholder for now', () => {
      const result = getHjsonContext('{ "queryType": "timeseries", "granularity": ');
      expect(result.currentObject).toEqual({ queryType: 'timeseries' });
    });

    it('includes currentObject in all contexts', () => {
      const result = getHjsonContext('{ "filter": { "type": ');
      expect(result).toHaveProperty('currentObject');
      expect(result.currentObject).toEqual({});
    });
  });
});
