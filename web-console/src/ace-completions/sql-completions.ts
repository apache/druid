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

import type { Ace } from 'ace-builds';
import { C, filterMap, N, T } from 'druid-query-toolkit';

import { SQL_CONSTANTS, SQL_DYNAMICS, SQL_KEYWORDS } from '../../lib/keywords';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../../lib/sql-docs';
import { DEFAULT_SERVER_QUERY_CONTEXT } from '../druid-models';
import type { ColumnMetadata } from '../utils';
import { lookupBy, uniq } from '../utils';

import { makeDocHtml } from './make-doc-html';

const SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_FUNCTION = [
  'AS',
  'ASC',
  'DESC',
  'LIMIT',
  'OFFSET',
  'RETURNING',
  'SET',
  'VALUES',
  'FETCH',
  'FIRST',
  'NEXT',
  'ONLY',
  'PRECEDING',
  'FOLLOWING',
  'UNBOUNDED',
];

const SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_REF = [
  'ASC',
  'DESC',
  'LIMIT',
  'OFFSET',
  'FIRST',
  'NEXT',
  'ONLY',
  'PRECEDING',
  'FOLLOWING',
  'UNBOUNDED',
  'CUBE',
  'ROLLUP',
  'FOR',
  'OUTER',
  'CROSS',
  'INNER',
  'LEFT',
  'RIGHT',
  'FULL',
  'NATURAL',
  'UNION',
  'INTERSECT',
  'EXCEPT',
];

const SQL_KEYWORD_FOLLOW_SUGGESTIONS: Record<string, string[]> = {
  // Keywords that can be followed by specific keywords
  SELECT: ['DISTINCT', 'ALL'],
  GROUP: ['BY'],
  ORDER: ['BY'],
  PARTITION: ['BY'],
  PARTITIONED: ['BY'],
  CLUSTERED: ['BY'],
  UNION: ['ALL'],
  INSERT: ['INTO'],
  REPLACE: ['INTO'],
  MERGE: ['INTO'],
  UPDATE: ['SET'],
  LEFT: ['JOIN', 'OUTER'],
  RIGHT: ['JOIN', 'OUTER'],
  INNER: ['JOIN'],
  FULL: ['JOIN', 'OUTER'],
  CROSS: ['JOIN'],
  NATURAL: ['JOIN', 'LEFT', 'RIGHT', 'INNER', 'FULL'],
  EXPLAIN: ['PLAN'],
  PLAN: ['FOR'],
  GROUPING: ['SETS'],
  FETCH: ['FIRST', 'NEXT'],
  FIRST: ['ROW', 'ROWS'],
  NEXT: ['ROW', 'ROWS'],
  ROWS: ['ONLY'],
  ROW: ['ONLY'],

  // Keywords that cannot be followed by any other keyword
  AS: [],
  ASC: [],
  DESC: [],
  LIMIT: [],
  OFFSET: [],
  RETURNING: [],
  SET: [],
  VALUES: [],
  ONLY: [],
  DISTINCT: [],
  ALL: [],
  WHERE: [],
  HAVING: [],
  USING: [],
  ON: [],
  CUBE: [],
  ROLLUP: [],
  OVERWRITE: [],
  PIVOT: [],
  UNPIVOT: [],
  MATCHED: [],
  PRECEDING: [],
  FOLLOWING: [],
  UNBOUNDED: [],
  CURRENT: [],
  EXTEND: [],
  WINDOW: [],
  RANGE: [],
  KEY: [],
  VALUE: [],
  TIME: [],
  EPOCH: [],
  MILLISECOND: [],
  SECOND: [],
  MINUTE: [],
  HOUR: [],
  DAY: [],
  DOW: [],
  ISODOW: [],
  DOY: [],
  WEEK: [],
  MONTH: [],
  QUARTER: [],
  YEAR: [],
  ISOYEAR: [],
  DECADE: [],
  CENTURY: [],
  MILLENNIUM: [],
};

const KNOWN_SQL_PARTS: Record<string, boolean> = {
  ...lookupBy(
    SQL_KEYWORDS.flatMap(k => k.split(/\s/g)), // The flatMap is needed because some keywords are like "EXPLAIN PLAN FOR"
    String,
    () => true,
  ),
  ...lookupBy(SQL_CONSTANTS, String, () => true),
  ...lookupBy(SQL_DYNAMICS, String, () => true),
  ...lookupBy(Object.values(SQL_DATA_TYPES), String, () => true),
  ...lookupBy(Object.values(SQL_FUNCTIONS), String, () => true),
};

export interface GetSqlCompletionsOptions {
  allText: string;
  keywordBeforePrefix: string | undefined;
  charBeforePrefix: string;
  prefix: string;
  columnMetadata?: readonly ColumnMetadata[];
  columns?: readonly string[];
}

export function getSqlCompletions({
  allText,
  keywordBeforePrefix,
  charBeforePrefix,
  prefix,
  columnMetadata,
  columns,
}: GetSqlCompletionsOptions): Ace.Completion[] {
  // If we are autocompleting inside a literal, then don't do any of the standard suggestions.
  // Only autocomplete other literals. The imagined use-case for this is if you have `country = 'France'` or `TIMESTAMP '2024-03-02 O1:00:00'` you might want to reuse the literals
  if (charBeforePrefix === "'") {
    return getSqlLiterals(allText, 100).map(value => ({
      value,
      score: 1,
      meta: 'local',
    }));
  }

  if (typeof keywordBeforePrefix === 'string') {
    keywordBeforePrefix = keywordBeforePrefix.toUpperCase();
  }

  console.log(keywordBeforePrefix);

  // Other than literals, do not autocomplete numbers
  if (/^\d+$/.test(prefix)) {
    return []; // Don't start completing if the user is typing a number
  }

  const possibleReferences = getPossibleSqlReferences(allText, 100);
  console.log(possibleReferences);

  let completions: Ace.Completion[] = possibleReferences.map(value => ({
    value,
    score: 1,
    meta: 'local',
  }));

  const quote = charBeforePrefix === '"';
  if (!quote) {
    completions = completions.concat(
      (SQL_KEYWORD_FOLLOW_SUGGESTIONS[keywordBeforePrefix || ''] || SQL_KEYWORDS).map(v => ({
        name: v,
        value: v,
        score: 10,
        meta: 'keyword',
      })),
      SQL_CONSTANTS.map(v => ({ name: v, value: v, score: 11, meta: 'constant' })),
      Object.entries(SQL_DATA_TYPES).map(([name, [runtime, description]]) => {
        return {
          name,
          value: name,
          score: 31,
          meta: 'type',
          docHTML: makeDocHtml({
            name,
            description,
            syntax: `Druid runtime type: ${runtime}`,
          }),
          docText: description,
        };
      }),
    );

    if (
      keywordBeforePrefix &&
      !SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_FUNCTION.includes(keywordBeforePrefix)
    ) {
      completions = completions.concat(
        SQL_DYNAMICS.map(v => ({ name: v, value: v, score: 20, meta: 'dynamic' })),
        Object.entries(SQL_FUNCTIONS).flatMap(([name, versions]) => {
          return versions.map(([args, description]) => {
            return {
              name,
              value: versions.length > 1 ? `${name}(${args})` : name,
              score: 30,
              meta: 'function',
              docHTML: makeDocHtml({ name, description, syntax: `${name}(${args})` }),
              docText: description,
              completer: {
                insertMatch: (editor: any, data: any) => {
                  editor.completer.insertMatch({ value: data.name });
                },
              },
            } as Ace.Completion;
          });
        }),
      );
    }
  }

  if (
    keywordBeforePrefix &&
    !SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_REF.includes(keywordBeforePrefix)
  ) {
    if (columnMetadata?.length) {
      const possibleReferencesLookup = lookupBy(possibleReferences, String, () => true);

      completions = completions.concat(
        uniq(columnMetadata.map(({ TABLE_SCHEMA }) => TABLE_SCHEMA)).map(schema => ({
          value: quote ? schema : String(N(schema)),
          score: 30,
          meta: 'schema',
        })),
        uniq(
          filterMap(columnMetadata, ({ TABLE_SCHEMA, TABLE_NAME }) =>
            TABLE_SCHEMA === 'druid' || possibleReferencesLookup[TABLE_SCHEMA]
              ? TABLE_NAME
              : undefined,
          ),
        ).map(table => ({
          value: quote ? table : String(T(table)),
          score: 40,
          meta: 'table',
        })),
        uniq(
          filterMap(columnMetadata, d =>
            possibleReferencesLookup[d.TABLE_NAME] ? d.COLUMN_NAME : undefined,
          ),
        ).map(v => ({
          value: quote ? v : String(C(v)),
          score: 50,
          meta: 'column',
        })),
      );
    }

    if (columns?.length) {
      completions = completions.concat(
        columns.map(column => ({
          value: quote ? column : String(C(column)),
          score: 50,
          meta: 'column',
        })),
      );
    }
  }

  if (keywordBeforePrefix === 'SET') {
    completions = completions.concat(
      Object.keys(DEFAULT_SERVER_QUERY_CONTEXT).map(key => ({
        value: quote ? key : String(C(key)),
        score: 50,
        meta: 'context',
      })),
    );
  }

  return completions;
}

export function getSqlLiterals(sqlText: string, maxWords: number): string[] {
  const literalRegexp = /'[^']{2,}'/g;
  const matches = (sqlText.match(literalRegexp) || []).map(stripOuterChars);

  return uniq(matches).slice(0, maxWords);
}

export function getPossibleSqlReferences(sqlText: string, maxWords: number): string[] {
  const quotedRegexp = /"\w+"/g;
  const quotedMatches = (sqlText.match(quotedRegexp) || []).map(stripOuterChars);

  const nakedRegexp = /[\s,([\-+*/][a-z]\w+[\s,)\]\-+*/]/gi;
  const nakedMatches = (sqlText.match(nakedRegexp) || []).map(stripOuterChars);

  return uniq([...quotedMatches, ...nakedMatches.filter(v => !KNOWN_SQL_PARTS[v])]).slice(
    0,
    maxWords,
  );
}

function stripOuterChars(str: string): string {
  return str.slice(1, str.length - 1);
}
