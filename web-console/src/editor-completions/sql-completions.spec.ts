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

import { dsql } from '../editor-languages/dsql';
import { completionContextAt } from '../test-utils/completion-context';
import type { ColumnMetadata } from '../utils';

import type { SqlCompletionOptions } from './sql-completions';
import { getPossibleSqlReferences, getSqlCompletions, getSqlLiterals } from './sql-completions';

// A state with the dsql language (for the syntax tree)
function dsqlState(sqlText: string): EditorState {
  return EditorState.create({ doc: sqlText, extensions: dsql() });
}

describe('sql-completions', () => {
  describe('getSqlCompletions', () => {
    // The completions with the cursor at the |
    function completionsAt(textWithCursor: string, options?: SqlCompletionOptions) {
      const [context, word] = completionContextAt(dsql(), textWithCursor);
      return getSqlCompletions(context, word, options);
    }

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

    it('returns empty array when in a single line comment', () => {
      expect(completionsAt('-- This is a comment SEL|')).toEqual([]);
    });

    it('returns empty array when typing after comment marker', () => {
      expect(completionsAt('SELECT * FROM table -- com|')).toEqual([]);
    });

    it('returns empty array when comment is in middle of line', () => {
      expect(completionsAt('SELECT -- some comment FR|')).toEqual([]);
    });

    it('returns empty array in block comments', () => {
      expect(completionsAt('SELECT /* FR| */ 1')).toEqual([]);
      expect(completionsAt('SELECT /*\n  FR|')).toEqual([]);
    });

    it('returns completions after a comment marker inside a literal', () => {
      const completions = completionsAt("SELECT 'a -- b' FR|");
      expect(completions.filter(c => c.detail === 'keyword').length).toBeGreaterThan(0);
    });

    it('returns empty array when prefix is a number', () => {
      expect(completionsAt('SELECT 123|')).toEqual([]);
    });

    it('returns completions before comment marker on same line', () => {
      const completions = completionsAt('SELECT FR| -- comment');
      expect(completions.length).toBeGreaterThan(0);
      const keywordCompletions = completions.filter(c => c.detail === 'keyword');
      expect(keywordCompletions.length).toBeGreaterThan(0);
    });

    it('returns empty array even when comment contains SQL keywords', () => {
      expect(completionsAt('-- TODO: need to SELECT * FROM WHE|')).toEqual([]);
    });

    it('returns empty array for indented comments', () => {
      expect(completionsAt('    -- indented comment FRO|')).toEqual([]);
    });

    it('returns only literals right after a single quote', () => {
      const completions = completionsAt(
        "SELECT * FROM table WHERE country = 'France' OR country = 'Germany' OR country = 'Fr|",
      );

      expect(completions).toEqual([
        { label: 'France', boost: 1, detail: 'local' },
        { label: 'Germany', boost: 1, detail: 'local' },
      ]);
    });

    it('does not return only literals right after a closed literal', () => {
      const completions = completionsAt("SELECT * FROM t WHERE country = 'France'|");
      expect(completions.some(c => c.detail === 'keyword')).toBe(true);
    });

    it('returns only literals anywhere inside a literal', () => {
      const completions = completionsAt(
        "SELECT * FROM table WHERE country = 'France' OR country = 'not Fra|'",
      );

      expect(completions.map(c => c.label)).toContain('France');
      expect(completions.every(c => c.detail === 'local')).toBe(true);
    });

    it('returns keyword suggestions after SELECT', () => {
      const completions = completionsAt('SELECT |');

      const keywordCompletions = completions.filter(c => c.detail === 'keyword');
      const keywordValues = keywordCompletions.map(c => c.label);

      expect(keywordValues).toContain('DISTINCT');
      expect(keywordValues).toContain('ALL');
      expect(keywordValues).not.toContain('SELECT');
    });

    it('looks at the keyword on an earlier line and past comments', () => {
      for (const sql of ['SELECT * FROM t GROUP\n  |', 'SELECT * FROM t GROUP /* by what */ |']) {
        expect(
          completionsAt(sql)
            .filter(c => c.detail === 'keyword')
            .map(c => c.label),
        ).toEqual(['BY']);
      }
    });

    it('looks at the keyword before a quote', () => {
      const completions = completionsAt('SELECT * FROM t LIMIT "|', { columns: ['page'] });
      expect(completions.filter(c => c.detail === 'column')).toHaveLength(0);
    });

    it('does not include functions after keywords that cannot be followed by functions', () => {
      const completions = completionsAt('SELECT * FROM t LIMIT |');

      const functionCompletions = completions.filter(c => c.detail === 'function');
      expect(functionCompletions).toHaveLength(0);

      const dynamicCompletions = completions.filter(c => c.detail === 'dynamic');
      expect(dynamicCompletions).toHaveLength(0);
    });

    it('includes functions after keywords that can be followed by functions', () => {
      const completions = completionsAt('SELECT * FROM t WHERE |');

      const functionCompletions = completions.filter(c => c.detail === 'function');
      expect(functionCompletions.length).toBeGreaterThan(0);

      const dynamicCompletions = completions.filter(c => c.detail === 'dynamic');
      expect(dynamicCompletions.length).toBeGreaterThan(0);
    });

    it('does not include column references after keywords that cannot be followed by refs', () => {
      const completions = completionsAt('SELECT * FROM "wikipedia" ORDER BY page DESC |', {
        columnMetadata,
      });

      const columnCompletions = completions.filter(c => c.detail === 'column');
      expect(columnCompletions).toHaveLength(0);

      const tableCompletions = completions.filter(c => c.detail === 'table');
      expect(tableCompletions).toHaveLength(0);
    });

    it('includes column references after keywords that can be followed by refs', () => {
      const completions = completionsAt('SELECT * FROM "wikipedia" WHERE |', { columnMetadata });

      const columnCompletions = completions.filter(c => c.detail === 'column');
      expect(columnCompletions.length).toBeGreaterThan(0);
      expect(columnCompletions.map(c => c.label)).toContain('"page"');
      expect(columnCompletions.map(c => c.label)).toContain('"user"');
    });

    it('includes context keys after SET keyword', () => {
      const completions = completionsAt('SET |');

      const contextCompletions = completions.filter(c => c.detail === 'context');
      expect(contextCompletions.length).toBeGreaterThan(0);
    });

    it('handles column names correctly with quotes', () => {
      const columns = ['column name with spaces', 'normal_column'];

      const completionsWithoutQuote = completionsAt('SELECT |', { columns });
      const completionsWithQuote = completionsAt('SELECT "|"', { columns });

      const columnWithoutQuote = completionsWithoutQuote.find(c =>
        c.label.includes('column name with spaces'),
      );
      expect(columnWithoutQuote?.label).toBe('"column name with spaces"');

      const columnWithQuote = completionsWithQuote.find(c => c.label === 'column name with spaces');
      expect(columnWithQuote?.label).toBe('column name with spaces');
    });

    it('includes constants and data types in completions', () => {
      const completions = completionsAt('|');

      const constantCompletions = completions.filter(c => c.detail === 'constant');
      expect(constantCompletions.map(c => c.label)).toContain('NULL');
      expect(constantCompletions.map(c => c.label)).toContain('TRUE');
      expect(constantCompletions.map(c => c.label)).toContain('FALSE');

      const typeCompletions = completions.filter(c => c.detail === 'type');
      expect(typeCompletions.length).toBeGreaterThan(0);
    });

    it('includes local references from the query text', () => {
      const completions = completionsAt('|SELECT my_column FROM my_table WHERE other_column = 1');

      const localCompletions = completions.filter(c => c.detail === 'local');
      const localValues = localCompletions.map(c => c.label);

      expect(localValues).toContain('my_column');
      expect(localValues).toContain('my_table');
      expect(localValues).toContain('other_column');
    });

    it('lists a function with several signatures once, with all of them in the doc', () => {
      const availableSqlFunctions = new Map([
        ['BIG_MAX', { args: ['<ANY>', 'column, size'], isAggregate: true }],
        ['MAX', { args: ['<COMPARABLE_TYPE>'], isAggregate: true }],
      ]);

      const functions = completionsAt('SELECT MAX|', { availableSqlFunctions }).filter(
        c => c.detail === 'aggregate',
      );

      expect(functions).toEqual([
        {
          label: 'BIG_MAX',
          boost: 30,
          detail: 'aggregate',
          doc: {
            name: 'BIG_MAX',
            syntax: 'BIG_MAX(<ANY>)\nBIG_MAX(column, size)',
            descriptionMarkdown: undefined,
          },
        },
        expect.objectContaining({
          label: 'MAX',
          doc: expect.objectContaining({ syntax: 'MAX(<COMPARABLE_TYPE>)' }),
        }),
      ]);
    });

    it('does not suggest the word that is being typed', () => {
      const references = completionsAt('SELECT my_column, my| FROM my_table')
        .filter(c => c.detail === 'local')
        .map(c => c.label);

      expect(references).toContain('my_column');
      expect(references).not.toContain('my');

      const literals = completionsAt(
        "SELECT * FROM t WHERE country = 'France' OR country = 'Fr|'",
      ).map(c => c.label);

      expect(literals).toEqual(['France']);
    });
  });

  describe('getSqlLiterals', () => {
    it('extracts string literals from SQL text', () => {
      const sqlText =
        "SELECT * FROM table WHERE country = 'France' OR city = 'Paris' OR code = 'FR'";
      const literals = getSqlLiterals(dsqlState(sqlText), 10);

      expect(literals).toEqual(['France', 'Paris', 'FR']);
    });

    it('respects maxWords limit', () => {
      const sqlText = "SELECT 'one', 'two', 'three', 'four', 'five'";
      const literals = getSqlLiterals(dsqlState(sqlText), 3);

      expect(literals).toHaveLength(3);
    });
  });

  describe('getPossibleSqlReferences', () => {
    it('extracts quoted identifiers', () => {
      const sqlText = 'SELECT "myColumn" FROM "myTable"';
      const references = getPossibleSqlReferences(dsqlState(sqlText), 10);

      expect(references).toContain('myColumn');
      expect(references).toContain('myTable');
    });

    it('extracts unquoted identifiers', () => {
      const sqlText = 'SELECT column1, column2 FROM table1 WHERE column3 = 1';
      const references = getPossibleSqlReferences(dsqlState(sqlText), 10);

      expect(references).toContain('column1');
      expect(references).toContain('column2');
      expect(references).toContain('table1');
      expect(references).toContain('column3');
    });

    it('filters out known SQL keywords', () => {
      const sqlText = 'SELECT column1 FROM table1 WHERE column2 = 1 ORDER BY column3';
      const references = getPossibleSqlReferences(dsqlState(sqlText), 20);

      expect(references).not.toContain('SELECT');
      expect(references).not.toContain('FROM');
      expect(references).not.toContain('WHERE');
      expect(references).not.toContain('ORDER');
      expect(references).not.toContain('BY');
    });

    it('finds identifiers next to any operator or punctuation', () => {
      const references = getPossibleSqlReferences(
        dsqlState('SELECT t.col1 FROM t WHERE col2=1 AND col3<>col4'),
        10,
      );

      expect(references).toEqual(['col1', 'col2', 'col3', 'col4']);
    });

    it('filters out keywords and functions in any case', () => {
      const references = getPossibleSqlReferences(
        dsqlState('select count(*) from table1 where column1 is not null'),
        10,
      );

      expect(references).toEqual(['table1', 'column1']);
    });

    it('skips quotes that are not closed', () => {
      expect(getPossibleSqlReferences(dsqlState('SELECT "col1", "col2'), 10)).toEqual(['col1']);
      expect(getSqlLiterals(dsqlState("SELECT 'one', 'two"), 10)).toEqual(['one']);
    });

    it('handles identifiers in expressions', () => {
      const sqlText = 'SELECT (column1 + column2) * column3 FROM table1';
      const references = getPossibleSqlReferences(dsqlState(sqlText), 10);

      expect(references).toContain('column1');
      expect(references).toContain('column2');
      expect(references).toContain('column3');
      expect(references).toContain('table1');
    });
  });
});
