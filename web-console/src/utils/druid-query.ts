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

import { C } from '@druid-toolkit/query';
import type { AxiosResponse } from 'axios';
import axios from 'axios';

import { Api } from '../singletons';

import type { RowColumn } from './general';
import { assemble } from './general';

const CANCELED_MESSAGE = 'Query canceled by user.';

// https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/error/DruidException.java#L292
export type ErrorResponsePersona = 'USER' | 'ADMIN' | 'OPERATOR' | 'DEVELOPER';

// https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/error/DruidException.java#L321
export type ErrorResponseCategory =
  | 'DEFENSIVE'
  | 'INVALID_INPUT'
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'CAPACITY_EXCEEDED'
  | 'CANCELED'
  | 'RUNTIME_FAILURE'
  | 'TIMEOUT'
  | 'UNSUPPORTED'
  | 'UNCATEGORIZED';

export interface ErrorResponse {
  persona: ErrorResponsePersona;
  category: ErrorResponseCategory;
  errorCode?: string;
  errorMessage: string; // a message for the intended audience
  context?: Record<string, any>; // a map of extra context values that might be helpful

  // Deprecated as per https://github.com/apache/druid/blob/master/processing/src/main/java/org/apache/druid/error/ErrorResponse.java
  error?: string;
  errorClass?: string;
  host?: string;
}

export interface QuerySuggestion {
  label: string;
  fn: (query: string) => string | undefined;
}

export function parseHtmlError(htmlStr: string): string | undefined {
  const startIndex = htmlStr.indexOf('</h3><pre>');
  const endIndex = htmlStr.indexOf('\n\tat');
  if (startIndex === -1 || endIndex === -1) return;

  return htmlStr
    .substring(startIndex + 10, endIndex)
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, `'`)
    .replace(/&gt;/g, '>');
}

function errorResponseFromWhatever(e: any): ErrorResponse | string {
  if (e.response) {
    // This is a direct axios response error
    let data = e.response.data || {};
    // MSQ errors nest their error objects inside the error key. Yo dawg, I heard you like errors...
    if (typeof data.error?.error === 'string') data = data.error;
    return data;
  } else {
    return e; // Assume the error was passed in directly
  }
}

export function getDruidErrorMessage(e: any): string {
  const data = errorResponseFromWhatever(e);
  switch (typeof data) {
    case 'object':
      return (
        assemble(
          data.error,
          data.errorMessage,
          data.errorClass,
          data.host ? `on host ${data.host}` : undefined,
        ).join(' / ') || e.message
      );

    case 'string': {
      const htmlResp = parseHtmlError(data);
      return htmlResp ? `HTML Error: ${htmlResp}` : e.message;
    }

    default:
      return e.message;
  }
}

export class DruidError extends Error {
  static extractStartRowColumn(
    context: Record<string, any> | undefined,
    offsetLines = 0,
  ): RowColumn | undefined {
    if (context?.sourceType !== 'sql' || !context.line || !context.column) return;

    return {
      row: Number(context.line) - 1 + offsetLines,
      column: Number(context.column) - 1,
    };
  }

  static extractEndRowColumn(
    context: Record<string, any> | undefined,
    offsetLines = 0,
  ): RowColumn | undefined {
    if (context?.sourceType !== 'sql' || !context.endLine || !context.endColumn) return;

    return {
      row: Number(context.endLine) - 1 + offsetLines,
      column: Number(context.endColumn) - 1,
    };
  }

  static positionToIndex(str: string, line: number, column: number): number {
    const lines = str.split('\n').slice(0, line);
    const lastLineIndex = lines.length - 1;
    lines[lastLineIndex] = lines[lastLineIndex].slice(0, column - 1);
    return lines.join('\n').length;
  }

  static getSuggestion(errorMessage: string): QuerySuggestion | undefined {
    // == is used instead of =
    // ex: SELECT * FROM wikipedia WHERE channel == '#en.wikipedia'
    // er: Received an unexpected token [= =] (line [1], column [39]), acceptable options:
    const matchEquals =
      /Received an unexpected token \[= =] \(line \[(\d+)], column \[(\d+)]\),/.exec(errorMessage);
    if (matchEquals) {
      const line = Number(matchEquals[1]);
      const column = Number(matchEquals[2]);
      return {
        label: `Replace == with =`,
        fn: str => {
          const index = DruidError.positionToIndex(str, line, column);
          if (!str.slice(index).startsWith('==')) return;
          return `${str.slice(0, index)}=${str.slice(index + 2)}`;
        },
      };
    }

    // Mangled quotes from copy/paste
    // ex: SELECT * FROM wikipedia WHERE channel = ‘#en.wikipedia‛
    // er: Lexical error at line 1, column 41.  Encountered: "\u2018"
    const matchLexical =
      /Lexical error at line (\d+), column (\d+).\s+Encountered: "\\u201\w"/.exec(errorMessage);
    if (matchLexical) {
      return {
        label: 'Replace fancy quotes with ASCII quotes',
        fn: str => {
          const newQuery = str
            .replace(/[\u2018-\u201b]/gim, `'`)
            .replace(/[\u201c-\u201f]/gim, `"`);
          if (newQuery === str) return;
          return newQuery;
        },
      };
    }

    // Incorrect quoting on table column
    // ex: SELECT * FROM wikipedia WHERE channel = "#en.wikipedia"
    // er: Column '#en.wikipedia' not found in any table (line [1], column [41])
    const matchQuotes =
      /Column '([^']+)' not found in any table \(line \[(\d+)], column \[(\d+)]\)/.exec(
        errorMessage,
      );
    if (matchQuotes) {
      const literalString = matchQuotes[1];
      const line = Number(matchQuotes[2]);
      const column = Number(matchQuotes[3]);
      return {
        label: `Replace "${literalString}" with '${literalString}'`,
        fn: str => {
          const index = DruidError.positionToIndex(str, line, column);
          if (!str.slice(index).startsWith(`"${literalString}"`)) return;
          return `${str.slice(0, index)}'${literalString}'${str.slice(
            index + literalString.length + 2,
          )}`;
        },
      };
    }

    // Single quotes on AS alias
    // ex: SELECT channel AS 'c' FROM wikipedia
    // er: Received an unexpected token [AS \'c\'] (line [1], column [16]), acceptable options:
    const matchSingleQuotesAlias = /Received an unexpected token \[AS \\'([\w-]+)\\']/i.exec(
      errorMessage,
    );
    if (matchSingleQuotesAlias) {
      const alias = matchSingleQuotesAlias[1];
      return {
        label: `Replace '${alias}' with "${alias}"`,
        fn: str => {
          const newQuery = str.replace(new RegExp(`(AS\\s*)'(${alias})'`, 'gim'), '$1"$2"');
          if (newQuery === str) return;
          return newQuery;
        },
      };
    }

    // Comma (,) before FROM, GROUP, ORDER, or LIMIT
    // ex: SELECT channel, FROM wikipedia
    // er: Received an unexpected token [, FROM] (line [1], column [15]), acceptable options:
    const matchComma = /Received an unexpected token \[, (FROM|GROUP|ORDER|LIMIT)]/i.exec(
      errorMessage,
    );
    if (matchComma) {
      const keyword = matchComma[1];
      return {
        label: `Remove comma (,) before ${keyword}`,
        fn: str => {
          const newQuery = str.replace(new RegExp(`,(\\s+${keyword})`, 'gim'), '$1');
          if (newQuery === str) return;
          return newQuery;
        },
      };
    }

    // Semicolon (;) at the end. https://bit.ly/1n1yfkJ
    // ex: SELECT 1;
    // ex: Received an unexpected token [;] (line [1], column [9]), acceptable options:
    const matchSemicolon =
      /Received an unexpected token \[;] \(line \[(\d+)], column \[(\d+)]\),/i.exec(errorMessage);
    if (matchSemicolon) {
      const line = Number(matchSemicolon[1]);
      const column = Number(matchSemicolon[2]);
      return {
        label: `Remove trailing semicolon (;)`,
        fn: str => {
          const index = DruidError.positionToIndex(str, line, column);
          if (str[index] !== ';') return;
          return str.slice(0, index) + str.slice(index + 1);
        },
      };
    }

    return;
  }

  public canceled?: boolean;
  public persona?: ErrorResponsePersona;
  public category?: ErrorResponseCategory;
  public context?: Record<string, any>;
  public errorMessage?: string;
  public errorMessageWithoutExpectation?: string;
  public expectation?: string;
  public startRowColumn?: RowColumn;
  public endRowColumn?: RowColumn;
  public suggestion?: QuerySuggestion;

  // Deprecated
  public error?: string;
  public errorClass?: string;
  public host?: string;

  constructor(e: any, offsetLines = 0) {
    super(axios.isCancel(e) ? CANCELED_MESSAGE : getDruidErrorMessage(e));
    if (axios.isCancel(e)) {
      this.canceled = true;
    } else {
      const data = errorResponseFromWhatever(e);

      let druidErrorResponse: ErrorResponse;
      switch (typeof data) {
        case 'object':
          druidErrorResponse = data;
          break;

        default:
          druidErrorResponse = {
            errorClass: 'HTML error',
          } as any; // ToDo
          break;
      }
      Object.assign(this, druidErrorResponse);

      if (this.errorMessage) {
        if (offsetLines) {
          this.errorMessage = this.errorMessage.replace(
            /line \[(\d+)],/g,
            (_, c) => `line [${Number(c) + offsetLines}],`,
          );
        }

        this.startRowColumn = DruidError.extractStartRowColumn(this.context, offsetLines);
        this.endRowColumn = DruidError.extractEndRowColumn(this.context, offsetLines);
        this.suggestion = DruidError.getSuggestion(this.errorMessage);

        const expectationIndex = this.errorMessage.indexOf('Was expecting one of');
        if (expectationIndex >= 0) {
          this.errorMessageWithoutExpectation = this.errorMessage.slice(0, expectationIndex).trim();
          this.expectation = this.errorMessage.slice(expectationIndex).trim();
        } else {
          this.errorMessageWithoutExpectation = this.errorMessage;
        }
      }
    }
  }
}

export async function queryDruidRune(runeQuery: Record<string, any>): Promise<any> {
  let runeResultResp: AxiosResponse;
  try {
    runeResultResp = await Api.instance.post('/druid/v2', runeQuery);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return runeResultResp.data;
}

export async function queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]> {
  let sqlResultResp: AxiosResponse;
  try {
    sqlResultResp = await Api.instance.post('/druid/v2/sql', sqlQueryPayload);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return sqlResultResp.data;
}

export interface QueryExplanation {
  query: any;
  signature: { name: string; type: string }[];
}

export function formatSignature(queryExplanation: QueryExplanation): string {
  return queryExplanation.signature
    .map(({ name, type }) => `${C.optionalQuotes(name)}::${type}`)
    .join(', ');
}
