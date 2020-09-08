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

import axios from 'axios';
import { AxiosResponse } from 'axios';

import { assemble } from './general';
import { RowColumn } from './query-cursor';

const CANCELED_MESSAGE = 'Query canceled by user.';

export interface DruidErrorResponse {
  error?: string;
  errorMessage?: string;
  errorClass?: string;
  host?: string;
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

  public canceled?: boolean;
  public error?: string;
  public errorMessage?: string;
  public position?: RowColumn;
  public errorClass?: string;
  public host?: string;

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
      }
    }
  }
}

export async function queryDruidRune(runeQuery: Record<string, any>): Promise<any> {
  let runeResultResp: AxiosResponse<any>;
  try {
    runeResultResp = await axios.post('/druid/v2', runeQuery);
  } catch (e) {
    throw new Error(getDruidErrorMessage(e));
  }
  return runeResultResp.data;
}

export async function queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]> {
  let sqlResultResp: AxiosResponse<any>;
  try {
    sqlResultResp = await axios.post('/druid/v2/sql', sqlQueryPayload);
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
