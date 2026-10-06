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

import { C, filterMap, N, T } from 'druid-query-toolkit';

import { SQL_CONSTANTS, SQL_DYNAMICS, SQL_KEYWORDS } from '../../lib/keywords';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../../lib/sql-docs';
import { DEFAULT_SERVER_QUERY_CONTEXT } from '../druid-models';
import type { AvailableFunctions } from '../helpers';
import type { ColumnMetadata } from '../utils';
import { lookupBy, uniq } from '../utils';

import type { EditorCompletion } from './editor-completion';

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
  IS: ['NOT', 'DISTINCT'],
  DISTINCT: ['FROM'],
  BY: ['GROUPING', 'ROLLUP', 'CUBE', 'ALL', 'HOUR', 'DAY', 'MONTH', 'YEAR'],
  ALL: ['TIME'],

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
  ...lookupBy(Array.from(SQL_DATA_TYPES.keys()), String, () => true),
  ...lookupBy(Array.from(SQL_FUNCTIONS.keys()), String, () => true),
};

export interface GetSqlCompletionsOptions {
  allText: string;
  lineBeforePrefix: string;
  charBeforePrefix: string;
  prefix: string;
  columnMetadata?: readonly ColumnMetadata[];
  columns?: readonly string[];
  availableSqlFunctions?: AvailableFunctions;
  skipAggregates?: boolean;
}

export function getSqlCompletions({
  allText,
  lineBeforePrefix,
  charBeforePrefix,
  prefix,
  columnMetadata,
  columns,
  availableSqlFunctions,
  skipAggregates,
}: GetSqlCompletionsOptions): EditorCompletion[] {
  // We are in a single line comment
  if (lineBeforePrefix.startsWith('--') || lineBeforePrefix.includes(' --')) {
    return [];
  }

  // If we are autocompleting inside a literal, then don't do any of the standard suggestions.
  // Only autocomplete other literals. The imagined use-case for this is if you have `country = 'France'` or `TIMESTAMP '2024-03-02 O1:00:00'` you might want to reuse the literals
  if (charBeforePrefix === "'") {
    return getSqlLiterals(allText, 100, prefix).map(label => ({
      label,
      boost: 1,
      detail: 'local',
    }));
  }

  const keywordBeforePrefix = (/(\w+)\s*$/.exec(lineBeforePrefix) || [])[1]?.toUpperCase();

  // Other than literals, do not autocomplete numbers
  if (/^\d+$/.test(prefix)) {
    return []; // Don't start completing if the user is typing a number
  }

  const possibleReferences = getPossibleSqlReferences(allText, 100, prefix);

  let completions: EditorCompletion[] = possibleReferences.map(label => ({
    label,
    boost: 1,
    detail: 'local',
  }));

  const quote = charBeforePrefix === '"';
  if (!quote) {
    completions = completions.concat(
      (SQL_KEYWORD_FOLLOW_SUGGESTIONS[keywordBeforePrefix || ''] || SQL_KEYWORDS).map(v => ({
        label: v,
        boost: 10,
        detail: 'keyword',
      })),
      SQL_CONSTANTS.map(v => ({ label: v, boost: 11, detail: 'constant' })),
      Array.from(SQL_DATA_TYPES.entries()).map(([name, [runtime, description]]) => {
        return {
          label: name,
          boost: 31,
          detail: 'type',
          doc: {
            name,
            syntax: `Druid runtime type: ${runtime}`,
            descriptionMarkdown: description,
          },
        };
      }),
    );

    if (
      !keywordBeforePrefix ||
      !SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_FUNCTION.includes(keywordBeforePrefix)
    ) {
      completions = completions.concat(
        SQL_DYNAMICS.map(v => ({ label: v, boost: 20, detail: 'dynamic' })),
      );

      // If availableSqlFunctions map is provided, use it; otherwise fall back to static SQL_FUNCTIONS
      if (availableSqlFunctions) {
        completions = completions.concat(
          Array.from(availableSqlFunctions.entries()).flatMap(([name, funcDef]) => {
            if (skipAggregates && funcDef.isAggregate) return [];
            const description = SQL_FUNCTIONS.get(name)?.[1];
            return funcDef.args.map(args => ({
              // Functions with several signatures are listed once per signature, but only the name is inserted
              label: name,
              displayLabel: funcDef.args.length > 1 ? `${name}(${args})` : undefined,
              boost: 30,
              detail: funcDef.isAggregate ? 'aggregate' : 'function',
              doc: { name, syntax: `${name}(${args})`, descriptionMarkdown: description },
            }));
          }),
        );
      } else {
        completions = completions.concat(
          Array.from(SQL_FUNCTIONS.entries()).map(([name, argDesc]) => {
            const [args, description] = argDesc;
            return {
              label: name,
              boost: 30,
              detail: 'function',
              doc: { name, syntax: `${name}(${args})`, descriptionMarkdown: description },
            };
          }),
        );
      }
    }
  }

  if (
    !keywordBeforePrefix ||
    !SQL_KEYWORDS_THAT_CAN_NOT_BE_FOLLOWED_BY_REF.includes(keywordBeforePrefix)
  ) {
    if (columnMetadata?.length) {
      const possibleReferencesLookup = lookupBy(possibleReferences, String, () => true);

      completions = completions.concat(
        uniq(columnMetadata.map(({ TABLE_SCHEMA }) => TABLE_SCHEMA)).map(schema => ({
          label: quote ? schema : String(N(schema)),
          boost: 30,
          detail: 'schema',
        })),
        uniq(
          filterMap(columnMetadata, ({ TABLE_SCHEMA, TABLE_NAME }) =>
            TABLE_SCHEMA === 'druid' || possibleReferencesLookup[TABLE_SCHEMA]
              ? TABLE_NAME
              : undefined,
          ),
        ).map(table => ({
          label: quote ? table : String(T(table)),
          boost: 40,
          detail: 'table',
        })),
        uniq(
          filterMap(columnMetadata, d =>
            possibleReferencesLookup[d.TABLE_NAME] ? d.COLUMN_NAME : undefined,
          ),
        ).map(v => ({
          label: quote ? v : String(C(v)),
          boost: 50,
          detail: 'column',
        })),
      );
    }

    if (columns?.length) {
      completions = completions.concat(
        columns.map(column => ({
          label: quote ? column : String(C(column)),
          boost: 50,
          detail: 'column',
        })),
      );
    }
  }

  if (keywordBeforePrefix === 'SET') {
    completions = completions.concat(
      Object.keys(DEFAULT_SERVER_QUERY_CONTEXT).map(key => ({
        label: quote ? key : String(C(key)),
        boost: 50,
        detail: 'context',
      })),
    );
  }

  return completions;
}

/**
 * The literals in the text. The prefix (the word being typed) is left out since it shows up as a literal itself.
 */
export function getSqlLiterals(sqlText: string, maxWords: number, prefix?: string): string[] {
  const literalRegexp = /'[^'\n]{2,}'/g;
  const matches = (sqlText.match(literalRegexp) || []).map(stripOuterChars);

  return uniq(matches.filter(m => m !== prefix)).slice(0, maxWords);
}

/**
 * The words in the text that could be references. The prefix (the word being typed) is left out since it shows up as a
 * reference itself.
 */
export function getPossibleSqlReferences(
  sqlText: string,
  maxWords: number,
  prefix?: string,
): string[] {
  const quotedRegexp = /"\w{2,}"/g;
  const quotedMatches = (sqlText.match(quotedRegexp) || []).map(stripOuterChars);

  // Match identifiers that are preceded by whitespace, comma, parenthesis, or operators
  // and followed by whitespace, comma, parenthesis, operators, or end of string, ensure length of at least 2 chars
  const nakedRegexp = /(?:^|[\s,([\-+*/])[a-zA-Z]\w+(?=[\s,)\]\-+*/]|$)/g;
  const nakedMatches = (sqlText.match(nakedRegexp) || []).map(s => s.replace(/^[\s,([\-+*/]/, ''));

  return uniq(
    [...quotedMatches, ...nakedMatches.filter(v => !KNOWN_SQL_PARTS[v])].filter(v => v !== prefix),
  ).slice(0, maxWords);
}

function stripOuterChars(str: string): string {
  return str.slice(1, str.length - 1);
}
