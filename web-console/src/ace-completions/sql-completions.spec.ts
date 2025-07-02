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

import type { ColumnMetadata } from '../utils';

import { getPossibleSqlReferences, getSqlCompletions, getSqlLiterals } from './sql-completions';

describe('sql-completions', () => {
  describe('getSqlCompletions', () => {
    const baseOptions = {
      allText: '',
      lineBeforePrefix: '',
      charBeforePrefix: '',
      prefix: '',
    };

    it('returns empty array when in a single line comment', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: '-- This is a comment',
        prefix: 'SEL',
      });
      expect(completions).toEqual([]);
    });

    it('returns empty array when typing after comment marker', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'SELECT * FROM table -- ',
        prefix: 'com',
      });
      expect(completions).toEqual([]);
    });

    it('returns empty array when comment is in middle of line', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'SELECT -- some comment',
        prefix: 'FR',
      });
      expect(completions).toEqual([]);
    });

    it('returns empty array when prefix is a number', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        prefix: '123',
      });
      expect(completions).toEqual([]);
    });

    it('returns completions before comment marker on same line', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'SELECT ',
        prefix: 'FR',
      });
      expect(completions.length).toBeGreaterThan(0);
      const keywordCompletions = completions.filter(c => c.meta === 'keyword');
      expect(keywordCompletions.length).toBeGreaterThan(0);
    });

    it('returns empty array even when comment contains SQL keywords', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: '-- TODO: need to SELECT * FROM',
        prefix: 'WHE',
      });
      expect(completions).toEqual([]);
    });

    it('returns empty array for indented comments', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: '    -- indented comment',
        prefix: 'FRO',
      });
      expect(completions).toEqual([]);
    });

    it('returns only literals when charBeforePrefix is single quote', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        allText: "SELECT * FROM table WHERE country = 'France' OR country = 'Germany'",
        charBeforePrefix: "'",
        prefix: 'Fr',
      });

      expect(completions).toHaveLength(2);
      expect(completions[0]).toEqual({
        value: 'France',
        score: 1,
        meta: 'local',
      });
      expect(completions[1]).toEqual({
        value: 'Germany',
        score: 1,
        meta: 'local',
      });
    });

    it('returns keyword suggestions after SELECT', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'SELECT',
      });

      const keywordCompletions = completions.filter(c => c.meta === 'keyword');
      const keywordValues = keywordCompletions.map(c => c.value);

      expect(keywordValues).toContain('DISTINCT');
      expect(keywordValues).toContain('ALL');
      expect(keywordValues).not.toContain('SELECT');
    });

    it('does not include functions after keywords that cannot be followed by functions', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'LIMIT',
      });

      const functionCompletions = completions.filter(c => c.meta === 'function');
      expect(functionCompletions).toHaveLength(0);

      const dynamicCompletions = completions.filter(c => c.meta === 'dynamic');
      expect(dynamicCompletions).toHaveLength(0);
    });

    it('includes functions after keywords that can be followed by functions', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'WHERE',
      });

      const functionCompletions = completions.filter(c => c.meta === 'function');
      expect(functionCompletions.length).toBeGreaterThan(0);

      const dynamicCompletions = completions.filter(c => c.meta === 'dynamic');
      expect(dynamicCompletions.length).toBeGreaterThan(0);
    });

    it('does not include column references after keywords that cannot be followed by refs', () => {
      const columnMetadata: ColumnMetadata[] = [
        {
          TABLE_SCHEMA: 'druid',
          TABLE_NAME: 'wikipedia',
          COLUMN_NAME: 'page',
          DATA_TYPE: 'VARCHAR',
        },
        {
          TABLE_SCHEMA: 'druid',
          TABLE_NAME: 'wikipedia',
          COLUMN_NAME: 'user',
          DATA_TYPE: 'VARCHAR',
        },
      ];

      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'DESC',
        columnMetadata,
      });

      const columnCompletions = completions.filter(c => c.meta === 'column');
      expect(columnCompletions).toHaveLength(0);

      const tableCompletions = completions.filter(c => c.meta === 'table');
      expect(tableCompletions).toHaveLength(0);
    });

    it('includes column references after keywords that can be followed by refs', () => {
      const columnMetadata: ColumnMetadata[] = [
        {
          TABLE_SCHEMA: 'druid',
          TABLE_NAME: 'wikipedia',
          COLUMN_NAME: 'page',
          DATA_TYPE: 'VARCHAR',
        },
        {
          TABLE_SCHEMA: 'druid',
          TABLE_NAME: 'wikipedia',
          COLUMN_NAME: 'user',
          DATA_TYPE: 'VARCHAR',
        },
      ];

      const completions = getSqlCompletions({
        ...baseOptions,
        allText: 'SELECT * FROM "wikipedia"',
        lineBeforePrefix: 'WHERE',
        columnMetadata,
      });

      const columnCompletions = completions.filter(c => c.meta === 'column');
      expect(columnCompletions.length).toBeGreaterThan(0);
      expect(columnCompletions.map(c => c.value)).toContain('"page"');
      expect(columnCompletions.map(c => c.value)).toContain('"user"');
    });

    it('includes context keys after SET keyword', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        lineBeforePrefix: 'SET',
      });

      const contextCompletions = completions.filter(c => c.meta === 'context');
      expect(contextCompletions.length).toBeGreaterThan(0);
    });

    it('handles column names correctly with quotes', () => {
      const columns = ['column name with spaces', 'normal_column'];

      const completionsWithoutQuote = getSqlCompletions({
        ...baseOptions,
        columns,
      });

      const completionsWithQuote = getSqlCompletions({
        ...baseOptions,
        charBeforePrefix: '"',
        columns,
      });

      const columnWithoutQuote = completionsWithoutQuote.find(c =>
        c.value.includes('column name with spaces'),
      );
      expect(columnWithoutQuote?.value).toBe('"column name with spaces"');

      const columnWithQuote = completionsWithQuote.find(c => c.value === 'column name with spaces');
      expect(columnWithQuote?.value).toBe('column name with spaces');
    });

    it('includes constants and data types in completions', () => {
      const completions = getSqlCompletions(baseOptions);

      const constantCompletions = completions.filter(c => c.meta === 'constant');
      expect(constantCompletions.map(c => c.value)).toContain('NULL');
      expect(constantCompletions.map(c => c.value)).toContain('TRUE');
      expect(constantCompletions.map(c => c.value)).toContain('FALSE');

      const typeCompletions = completions.filter(c => c.meta === 'type');
      expect(typeCompletions.length).toBeGreaterThan(0);
    });

    it('includes local references from the query text', () => {
      const completions = getSqlCompletions({
        ...baseOptions,
        allText: 'SELECT my_column FROM my_table WHERE other_column = 1',
      });

      const localCompletions = completions.filter(c => c.meta === 'local');
      const localValues = localCompletions.map(c => c.value);

      expect(localValues).toContain('my_column');
      expect(localValues).toContain('my_table');
      expect(localValues).toContain('other_column');
    });
  });

  describe('getSqlLiterals', () => {
    it('extracts string literals from SQL text', () => {
      const sqlText =
        "SELECT * FROM table WHERE country = 'France' OR city = 'Paris' OR code = 'FR'";
      const literals = getSqlLiterals(sqlText, 10);

      expect(literals).toEqual(['France', 'Paris', 'FR']);
    });

    it('respects maxWords limit', () => {
      const sqlText = "SELECT 'one', 'two', 'three', 'four', 'five'";
      const literals = getSqlLiterals(sqlText, 3);

      expect(literals).toHaveLength(3);
    });
  });

  describe('getPossibleSqlReferences', () => {
    it('extracts quoted identifiers', () => {
      const sqlText = 'SELECT "myColumn" FROM "myTable"';
      const references = getPossibleSqlReferences(sqlText, 10);

      expect(references).toContain('myColumn');
      expect(references).toContain('myTable');
    });

    it('extracts unquoted identifiers', () => {
      const sqlText = 'SELECT column1, column2 FROM table1 WHERE column3 = 1';
      const references = getPossibleSqlReferences(sqlText, 10);

      expect(references).toContain('column1');
      expect(references).toContain('column2');
      expect(references).toContain('table1');
      expect(references).toContain('column3');
    });

    it('filters out known SQL keywords', () => {
      const sqlText = 'SELECT column1 FROM table1 WHERE column2 = 1 ORDER BY column3';
      const references = getPossibleSqlReferences(sqlText, 20);

      expect(references).not.toContain('SELECT');
      expect(references).not.toContain('FROM');
      expect(references).not.toContain('WHERE');
      expect(references).not.toContain('ORDER');
      expect(references).not.toContain('BY');
    });

    it('handles identifiers in expressions', () => {
      const sqlText = 'SELECT (column1 + column2) * column3 FROM table1';
      const references = getPossibleSqlReferences(sqlText, 10);

      expect(references).toContain('column1');
      expect(references).toContain('column2');
      expect(references).toContain('column3');
      expect(references).toContain('table1');
    });
  });
});
