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

import type { JsonCompletionRule } from './json-completion';
import { getCompletionsForPath } from './json-completion';

describe('json-completion', () => {
  describe('getCompletionsForPath', () => {
    const mockRules: JsonCompletionRule[] = [
      // Root level object properties
      {
        path: '$',
        isObject: true,
        completions: [
          { value: 'name', documentation: 'The name property' },
          { value: 'type', documentation: 'The type property' },
        ],
      },
      // Root level values
      {
        path: '$.type',
        completions: [
          { value: 'A', documentation: 'Type A' },
          { value: 'B', documentation: 'Type B' },
        ],
      },
      // Nested object properties
      {
        path: '$.config',
        isObject: true,
        completions: [
          { value: 'enabled', documentation: 'Enable the feature' },
          { value: 'timeout', documentation: 'Timeout in milliseconds' },
        ],
      },
      // Array item properties
      {
        path: '$.items.[]',
        isObject: true,
        completions: [
          { value: 'id', documentation: 'Item ID' },
          { value: 'value', documentation: 'Item value' },
        ],
      },
      // Conditional completion
      {
        path: '$',
        isObject: true,
        condition: obj => obj.type === 'A',
        completions: [{ value: 'specialPropertyForA', documentation: 'Only for type A' }],
      },
      // RegExp path
      {
        path: /^\$\.dynamic\.[^.]+$/,
        completions: [{ value: 'dynamicValue', documentation: 'Dynamic property value' }],
      },
      // Deep nested path
      {
        path: '$.level1.level2.level3',
        isObject: true,
        completions: [{ value: 'deepProperty', documentation: 'Deep nested property' }],
      },
    ];

    it('should return root level object completions for keys', () => {
      const completions = getCompletionsForPath(mockRules, [], true, {});
      expect(completions).toEqual([
        { value: 'name', documentation: 'The name property' },
        { value: 'type', documentation: 'The type property' },
      ]);
    });

    it('should return value completions for specific paths', () => {
      const completions = getCompletionsForPath(mockRules, ['type'], false, {});
      expect(completions).toEqual([
        { value: 'A', documentation: 'Type A' },
        { value: 'B', documentation: 'Type B' },
      ]);
    });

    it('should return nested object completions', () => {
      const completions = getCompletionsForPath(mockRules, ['config'], true, {});
      expect(completions).toEqual([
        { value: 'enabled', documentation: 'Enable the feature' },
        { value: 'timeout', documentation: 'Timeout in milliseconds' },
      ]);
    });

    it('should handle array paths correctly', () => {
      const completions = getCompletionsForPath(mockRules, ['items', '0'], true, {});
      expect(completions).toEqual([
        { value: 'id', documentation: 'Item ID' },
        { value: 'value', documentation: 'Item value' },
      ]);
    });

    it('should handle multiple array indices in path', () => {
      const completions = getCompletionsForPath(mockRules, ['items', '0'], true, {});
      expect(completions).toEqual([
        { value: 'id', documentation: 'Item ID' },
        { value: 'value', documentation: 'Item value' },
      ]);
    });

    it('should apply conditional completions when condition is met', () => {
      const completions = getCompletionsForPath(mockRules, [], true, { type: 'A' });
      expect(completions).toContainEqual({
        value: 'specialPropertyForA',
        documentation: 'Only for type A',
      });
    });

    it('should not apply conditional completions when condition is not met', () => {
      const completions = getCompletionsForPath(mockRules, [], true, { type: 'B' });
      expect(completions).not.toContainEqual({
        value: 'specialPropertyForA',
        documentation: 'Only for type A',
      });
    });

    it('should match RegExp paths', () => {
      const completions = getCompletionsForPath(mockRules, ['dynamic', 'someProperty'], false, {});
      expect(completions).toEqual([
        { value: 'dynamicValue', documentation: 'Dynamic property value' },
      ]);
    });

    it('should not match RegExp paths that do not match', () => {
      const completions = getCompletionsForPath(
        mockRules,
        ['dynamic', 'prop1', 'prop2'],
        false,
        {},
      );
      expect(completions).toEqual([]);
    });

    it('should handle deep nested paths', () => {
      const completions = getCompletionsForPath(
        mockRules,
        ['level1', 'level2', 'level3'],
        true,
        {},
      );
      expect(completions).toEqual([
        { value: 'deepProperty', documentation: 'Deep nested property' },
      ]);
    });

    it('should return empty array when no rules match', () => {
      const completions = getCompletionsForPath(mockRules, ['nonexistent', 'path'], true, {});
      expect(completions).toEqual([]);
    });

    it('should distinguish between key and value completions', () => {
      // Looking for keys (isObject: true)
      const keyCompletions = getCompletionsForPath(mockRules, [], true, {});
      expect(keyCompletions.map(c => c.value)).toContain('name');

      // Looking for values (isObject: false or undefined)
      const valueCompletions = getCompletionsForPath(mockRules, [], false, {});
      expect(valueCompletions).toEqual([]);
    });

    it('should combine completions from multiple matching rules', () => {
      const multipleMatchRules: JsonCompletionRule[] = [
        {
          path: '$',
          isObject: true,
          completions: [{ value: 'prop1' }],
        },
        {
          path: '$',
          isObject: true,
          completions: [{ value: 'prop2' }],
        },
        {
          path: '$',
          isObject: true,
          condition: () => true,
          completions: [{ value: 'prop3' }],
        },
      ];

      const completions = getCompletionsForPath(multipleMatchRules, [], true, {});
      expect(completions).toHaveLength(3);
      expect(completions.map(c => c.value)).toEqual(['prop1', 'prop2', 'prop3']);
    });

    it('should handle empty rules array', () => {
      const completions = getCompletionsForPath([], ['any', 'path'], true, {});
      expect(completions).toEqual([]);
    });

    it('should handle empty path array', () => {
      const completions = getCompletionsForPath(mockRules, [], true, {});
      expect(completions).toEqual([
        { value: 'name', documentation: 'The name property' },
        { value: 'type', documentation: 'The type property' },
      ]);
    });

    it('should handle complex array paths', () => {
      const complexRules: JsonCompletionRule[] = [
        {
          path: '$.matrix.[].[]',
          isObject: true,
          completions: [{ value: 'cell' }],
        },
        {
          path: '$.data.[].items.[].value',
          completions: [{ value: 'itemValue' }],
        },
      ];

      // Test matrix path
      const matrixCompletions = getCompletionsForPath(complexRules, ['matrix', '0', '1'], true, {});
      expect(matrixCompletions).toEqual([{ value: 'cell' }]);

      // Test nested array path
      const nestedCompletions = getCompletionsForPath(
        complexRules,
        ['data', '2', 'items', '5', 'value'],
        false,
        {},
      );
      expect(nestedCompletions).toEqual([{ value: 'itemValue' }]);
    });

    it('should handle paths with special characters', () => {
      const specialRules: JsonCompletionRule[] = [
        {
          path: '$.config-name',
          isObject: true,
          completions: [{ value: 'sub-property' }],
        },
        {
          path: '$["special.property"]',
          completions: [{ value: 'special-value' }],
        },
      ];

      const completions = getCompletionsForPath(specialRules, ['config-name'], true, {});
      expect(completions).toEqual([{ value: 'sub-property' }]);
    });
  });

  describe('pathToString', () => {
    // Since pathToString is not exported, we test it indirectly through getCompletionsForPath
    it('should convert array indices to [] notation', () => {
      const rules: JsonCompletionRule[] = [
        {
          path: '$.items.[]',
          isObject: true,
          completions: [{ value: 'test' }],
        },
      ];

      // Test single array index
      let completions = getCompletionsForPath(rules, ['items', '0'], true, {});
      expect(completions).toHaveLength(1);

      // Test different array index
      completions = getCompletionsForPath(rules, ['items', '99'], true, {});
      expect(completions).toHaveLength(1);

      // Test non-array path
      completions = getCompletionsForPath(rules, ['items', 'notAnIndex'], true, {});
      expect(completions).toHaveLength(0);
    });

    it('should handle multiple array indices', () => {
      const rules: JsonCompletionRule[] = [
        {
          path: '$.data.[].items.[]',
          isObject: true,
          completions: [{ value: 'test' }],
        },
      ];

      const completions = getCompletionsForPath(rules, ['data', '0', 'items', '5'], true, {});
      expect(completions).toHaveLength(1);
    });

    it('should handle paths starting with array index', () => {
      const rules: JsonCompletionRule[] = [
        {
          path: '$.[].property',
          isObject: true,
          completions: [{ value: 'test' }],
        },
      ];

      const completions = getCompletionsForPath(rules, ['0', 'property'], true, {});
      expect(completions).toHaveLength(1);
    });
  });
});
