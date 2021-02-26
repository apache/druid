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

import axios, { AxiosResponse } from 'axios';

import { Api } from '../singletons';

import { assemble } from './general';
import { RowColumn } from './query-cursor';

const CANCELED_MESSAGE = 'Query canceled by user.';

export interface DruidErrorResponse {
  error?: string;
  errorMessage?: string;
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

export function getDruidErrorMessage(e: any): string {
  const data: DruidErrorResponse | string = (e.response || {}).data || {};
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

    case 'string':
      const htmlResp = parseHtmlError(data);
      return htmlResp ? `HTML Error: ${htmlResp}` : e.message;

    default:
      return e.message;
  }
}

export class DruidError extends Error {
  static parsePosition(errorMessage: string): RowColumn | undefined {
    const range = String(errorMessage).match(
      /from line (\d+), column (\d+) to line (\d+), column (\d+)/i,
    );
    if (range) {
      return {
        match: range[0],
        row: Number(range[1]) - 1,
        column: Number(range[2]) - 1,
        endRow: Number(range[3]) - 1,
        endColumn: Number(range[4]), // No -1 because we need to include the last char
      };
    }

    const single = String(errorMessage).match(/at line (\d+), column (\d+)/i);
    if (single) {
      return {
        match: single[0],
        row: Number(single[1]) - 1,
        column: Number(single[2]) - 1,
      };
    }

    return;
  }

  static positionToIndex(str: string, line: number, column: number): number {
    const lines = str.split('\n').slice(0, line);
    const lastLineIndex = lines.length - 1;
    lines[lastLineIndex] = lines[lastLineIndex].slice(0, column - 1);
    return lines.join('\n').length;
  }

  static getSuggestion(errorMessage: string): QuerySuggestion | undefined {
    // == is used instead of =
    // ex: Encountered "= =" at line 3, column 15. Was expecting one of
    const matchEquals = errorMessage.match(/Encountered "= =" at line (\d+), column (\d+)./);
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

    // Incorrect quoting on table
    // ex: org.apache.calcite.runtime.CalciteContextException: From line 3, column 17 to line 3, column 31: Column '#ar.wikipedia' not found in any table
    const matchQuotes = errorMessage.match(
      /org.apache.calcite.runtime.CalciteContextException: From line (\d+), column (\d+) to line \d+, column \d+: Column '([^']+)' not found in any table/,
    );
    if (matchQuotes) {
      const line = Number(matchQuotes[1]);
      const column = Number(matchQuotes[2]);
      const literalString = matchQuotes[3];
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

    // , before FROM
    const matchComma = errorMessage.match(/Encountered "(FROM)" at/i);
    if (matchComma) {
      const fromKeyword = matchComma[1];
      return {
        label: `Remove , before ${fromKeyword}`,
        fn: str => {
          const newQuery = str.replace(/,(\s+FROM)/gim, '$1');
          if (newQuery === str) return;
          return newQuery;
        },
      };
    }

    return;
  }

  public canceled?: boolean;
  public error?: string;
  public errorMessage?: string;
  public errorMessageWithoutExpectation?: string;
  public expectation?: string;
  public position?: RowColumn;
  public errorClass?: string;
  public host?: string;
  public suggestion?: QuerySuggestion;

  constructor(e: any) {
    super(axios.isCancel(e) ? CANCELED_MESSAGE : getDruidErrorMessage(e));
    if (axios.isCancel(e)) {
      this.canceled = true;
    } else {
      const data: DruidErrorResponse | string = (e.response || {}).data || {};

      let druidErrorResponse: DruidErrorResponse;
      switch (typeof data) {
        case 'object':
          druidErrorResponse = data;
          break;

        case 'string':
          druidErrorResponse = {
            errorClass: 'HTML error',
          };
          break;

        default:
          druidErrorResponse = {};
          break;
      }
      Object.assign(this, druidErrorResponse);

      if (this.errorMessage) {
        this.position = DruidError.parsePosition(this.errorMessage);
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
  let runeResultResp: AxiosResponse<any>;
  try {
    runeResultResp = await Api.instance.post('/druid/v2', runeQuery);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return runeResultResp.data;
}

export async function queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]> {
  let sqlResultResp: AxiosResponse<any>;
  try {
    sqlResultResp = await Api.instance.post('/druid/v2/sql', sqlQueryPayload);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return sqlResultResp.data;
}

export interface BasicQueryExplanation {
  query: any;
  signature: string | null;
}

export interface SemiJoinQueryExplanation {
  mainQuery: BasicQueryExplanation;
  subQueryRight: BasicQueryExplanation;
}

function parseQueryPlanResult(queryPlanResult: string): BasicQueryExplanation {
  if (!queryPlanResult) {
    return {
      query: null,
      signature: null,
    };
  }

  const queryAndSignature = queryPlanResult.split(', signature=');
  const queryValue = new RegExp(/query=(.+)/).exec(queryAndSignature[0]);
  const signatureValue = queryAndSignature[1];

  let parsedQuery: any;

  if (queryValue && queryValue[1]) {
    try {
      parsedQuery = JSON.parse(queryValue[1]);
    } catch (e) {}
  }

  return {
    query: parsedQuery || queryPlanResult,
    signature: signatureValue || null,
  };
}

export function parseQueryPlan(
  raw: string,
): BasicQueryExplanation | SemiJoinQueryExplanation | string {
  let plan: string = raw;
  plan = plan.replace(/\n/g, '');

  if (plan.includes('DruidOuterQueryRel(')) {
    return plan; // don't know how to parse this
  }

  let queryArgs: string;
  const queryRelFnStart = 'DruidQueryRel(';
  const semiJoinFnStart = 'DruidSemiJoin(';

  if (plan.startsWith(queryRelFnStart)) {
    queryArgs = plan.substring(queryRelFnStart.length, plan.length - 1);
  } else if (plan.startsWith(semiJoinFnStart)) {
    queryArgs = plan.substring(semiJoinFnStart.length, plan.length - 1);
    const leftExpressionsArgs = ', leftExpressions=';
    const keysArgumentIdx = queryArgs.indexOf(leftExpressionsArgs);
    if (keysArgumentIdx !== -1) {
      return {
        mainQuery: parseQueryPlanResult(queryArgs.substring(0, keysArgumentIdx)),
        subQueryRight: parseQueryPlan(queryArgs.substring(queryArgs.indexOf(queryRelFnStart))),
      } as SemiJoinQueryExplanation;
    }
  } else {
    return plan;
  }

  return parseQueryPlanResult(queryArgs);
}
