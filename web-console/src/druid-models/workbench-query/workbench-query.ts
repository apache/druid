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

import type {
  QueryParameter,
  SqlClusteredByClause,
  SqlExpression,
  SqlPartitionedByClause,
} from '@druid-toolkit/query';
import {
  C,
  F,
  SqlLiteral,
  SqlOrderByClause,
  SqlOrderByExpression,
  SqlQuery,
} from '@druid-toolkit/query';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import { v4 as uuidv4 } from 'uuid';

import type { RowColumn } from '../../utils';
import { deleteKeys } from '../../utils';
import type { DruidEngine } from '../druid-engine/druid-engine';
import { validDruidEngine } from '../druid-engine/druid-engine';
import type { LastExecution } from '../execution/execution';
import { validateLastExecution } from '../execution/execution';
import type { ExternalConfig } from '../external-config/external-config';
import {
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
} from '../ingest-query-pattern/ingest-query-pattern';
import type { ArrayMode } from '../ingestion-spec/ingestion-spec';
import type { QueryContext } from '../query-context/query-context';

const ISSUE_MARKER = '--:ISSUE:';

export interface TabEntry {
  id: string;
  tabName: string;
  query: WorkbenchQuery;
}

interface IngestionLines {
  insertReplaceLine?: string;
  overwriteLine?: string;
  partitionedByLine?: string;
  clusteredByLine?: string;
}

// -----------------------------

export interface WorkbenchQueryValue {
  queryString: string;
  queryContext: QueryContext;
  queryParameters?: QueryParameter[];
  engine?: DruidEngine;
  lastExecution?: LastExecution;
  unlimited?: boolean;
  prefixLines?: number;

  // Legacy
  queryParts?: any[];
}

export class WorkbenchQuery {
  private static enabledQueryEngines: DruidEngine[] = ['native', 'sql-native'];

  static blank(): WorkbenchQuery {
    return new WorkbenchQuery({
      queryString: '',
      queryContext: {},
    });
  }

  static fromInitExternalConfig(
    externalConfig: ExternalConfig,
    timeExpression: SqlExpression | undefined,
    partitionedByHint: string | undefined,
    arrayMode: ArrayMode,
  ): WorkbenchQuery {
    return new WorkbenchQuery({
      queryString: ingestQueryPatternToQuery(
        externalConfigToIngestQueryPattern(
          externalConfig,
          timeExpression,
          partitionedByHint,
          arrayMode,
        ),
      ).toString(),
      queryContext: {
        arrayIngestMode: 'array',
      },
    });
  }

  static fromString(tabString: string): WorkbenchQuery {
    const parts = tabString.split('\n\n');
    const headers: string[] = [];
    const bodies: string[] = [];
    for (const part of parts) {
      const m = part.match(/^===== (Helper:.+|Query|Context) =====$/);
      if (m) {
        headers.push(m[1]);
      } else {
        const i = headers.length - 1;
        if (i < 0) throw new Error('content before header');
        bodies[i] = bodies[i] ? bodies[i] + '\n\n' + part : part;
      }
    }

    let queryString = '';
    let queryContext: QueryContext = {};
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i];
      const body = bodies[i];
      if (header === 'Context') {
        queryContext = JSONBig.parse(body);
      } else {
        queryString = body;
      }
    }

    return new WorkbenchQuery({
      queryString,
      queryContext,
    });
  }

  static setQueryEngines(queryEngines: DruidEngine[]): void {
    WorkbenchQuery.enabledQueryEngines = queryEngines;
  }

  static getQueryEngines(): DruidEngine[] {
    return WorkbenchQuery.enabledQueryEngines;
  }

  static fromEffectiveQueryAndContext(queryString: string, context: QueryContext): WorkbenchQuery {
    const noSqlOuterLimit = typeof context['sqlOuterLimit'] === 'undefined';
    const cleanContext = deleteKeys(context, ['sqlOuterLimit']);

    let retQuery = WorkbenchQuery.blank()
      .changeQueryString(queryString)
      .changeQueryContext(cleanContext);

    if (noSqlOuterLimit && !retQuery.isIngestQuery()) {
      retQuery = retQuery.changeUnlimited(true);
    }

    return retQuery;
  }

  static getIngestionLines(sqlString: string): IngestionLines {
    const lines = sqlString.split('\n');
    return {
      insertReplaceLine: lines.find(line => /^\s*(?:INSERT|REPLACE)\s+INTO/i.test(line)),
      overwriteLine: lines.find(line => /^\s*OVERWRITE/i.test(line)),
      partitionedByLine: lines.find(line => /^\s*PARTITIONED\s+BY/i.test(line)),
      clusteredByLine: lines.find(line => /^\s*CLUSTERED\s+BY/i.test(line)),
    };
  }

  static commentOutIngestParts(sqlString: string): string {
    return sqlString
      .split('\n')
      .map(line =>
        line.replace(
          /^(\s*)(INSERT\s+INTO|REPLACE\s+INTO|OVERWRITE|PARTITIONED\s+BY|CLUSTERED\s+BY)/i,
          (_, spaces, thing) => `${spaces}--${thing.slice(2)}`,
        ),
      )
      .join('\n');
  }

  static makeOrderByClause(
    partitionedByClause: SqlPartitionedByClause | undefined,
    clusteredByClause: SqlClusteredByClause | undefined,
  ): SqlOrderByClause | undefined {
    if (!partitionedByClause) return;

    const orderByExpressions: SqlOrderByExpression[] = [];
    let partitionedByExpression = partitionedByClause.expression;
    if (partitionedByExpression) {
      if (partitionedByExpression instanceof SqlLiteral) {
        partitionedByExpression = F.floor(C('__time'), partitionedByExpression);
      }
      orderByExpressions.push(SqlOrderByExpression.create(partitionedByExpression));
    }

    if (clusteredByClause) {
      orderByExpressions.push(
        ...clusteredByClause.expressions.values.map(ex => SqlOrderByExpression.create(ex)),
      );
    }

    return orderByExpressions.length ? SqlOrderByClause.create(orderByExpressions) : undefined;
  }

  static getRowColumnFromIssue(issue: string): RowColumn | undefined {
    const m = issue.match(/at line (\d+),(\d+)/);
    if (!m) return;
    return { row: Number(m[1]) - 1, column: Number(m[2]) - 1 };
  }

  static isTaskEngineNeeded(queryString: string): boolean {
    return /EXTERN\s*\(|(?:INSERT|REPLACE)\s+INTO/im.test(queryString);
  }

  public readonly queryString: string;
  public readonly queryContext: QueryContext;
  public readonly queryParameters?: QueryParameter[];
  public readonly engine?: DruidEngine;
  public readonly lastExecution?: LastExecution;
  public readonly unlimited?: boolean;
  public readonly prefixLines?: number;

  public readonly parsedQuery?: SqlQuery;

  constructor(value: WorkbenchQueryValue) {
    let queryString = value.queryString;
    // Back compat to read legacy workbench query
    if (typeof queryString === 'undefined' && Array.isArray(value.queryParts)) {
      const lastQueryPart = value.queryParts[value.queryParts.length - 1];
      queryString = lastQueryPart.queryString || '';
    }
    this.queryString = queryString;
    this.queryContext = value.queryContext;
    this.queryParameters = value.queryParameters;

    // Start back compat code for the engine names that might be coming from local storage
    let possibleEngine: string | undefined = value.engine;
    if (possibleEngine === 'sql') {
      possibleEngine = 'sql-native';
    } else if (possibleEngine === 'sql-task') {
      possibleEngine = 'sql-msq-task';
    }
    // End bac compat code

    this.engine = validDruidEngine(possibleEngine) ? possibleEngine : undefined;
    this.lastExecution = validateLastExecution(value.lastExecution);

    if (value.unlimited) this.unlimited = true;
    this.prefixLines = value.prefixLines;

    this.parsedQuery = SqlQuery.maybeParse(this.queryString);
  }

  public valueOf(): WorkbenchQueryValue {
    return {
      queryString: this.queryString,
      queryContext: this.queryContext,
      queryParameters: this.queryParameters,
      engine: this.engine,
      unlimited: this.unlimited,
    };
  }

  public toString(): string {
    const { queryString, queryContext } = this;
    return [
      `===== Query =====`,
      queryString,
      `===== Context =====`,
      JSONBig.stringify(queryContext, undefined, 2),
    ].join('\n\n');
  }

  public changeQueryString(queryString: string): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryString });
  }

  public changeQueryContext(queryContext: QueryContext): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryContext });
  }

  public changeQueryParameters(queryParameters: QueryParameter[] | undefined): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryParameters });
  }

  public changeEngine(engine: DruidEngine | undefined): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), engine });
  }

  public changeLastExecution(lastExecution: LastExecution | undefined): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), lastExecution });
  }

  public changeUnlimited(unlimited: boolean): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), unlimited });
  }

  public changePrefixLines(prefixLines: number): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), prefixLines });
  }

  public isTaskEngineNeeded(): boolean {
    return WorkbenchQuery.isTaskEngineNeeded(this.queryString);
  }

  public getEffectiveEngine(): DruidEngine {
    const { engine } = this;
    if (engine) return engine;
    const enabledEngines = WorkbenchQuery.getQueryEngines();
    if (this.isJsonLike()) {
      if (this.isSqlInJson()) {
        if (enabledEngines.includes('sql-native')) return 'sql-native';
      } else {
        if (enabledEngines.includes('native')) return 'native';
      }
    }
    if (enabledEngines.includes('sql-msq-task') && this.isTaskEngineNeeded()) return 'sql-msq-task';
    if (enabledEngines.includes('sql-native')) return 'sql-native';
    return enabledEngines[0] || 'sql-native';
  }

  public getQueryString(): string {
    return this.queryString;
  }

  public getLastExecution(): LastExecution | undefined {
    return this.lastExecution;
  }

  public getParsedQuery(): SqlQuery | undefined {
    return this.parsedQuery;
  }

  public isEmptyQuery(): boolean {
    return this.queryString.trim() === '';
  }

  public getIssue(): string | undefined {
    if (this.isJsonLike()) {
      return this.issueWithJson();
    }
    return;
  }

  public isJsonLike(): boolean {
    return this.queryString.trim().startsWith('{');
  }

  public issueWithJson(): string | undefined {
    try {
      Hjson.parse(this.queryString);
    } catch (e) {
      return e.message;
    }
    return;
  }

  public isSqlInJson(): boolean {
    try {
      const query = Hjson.parse(this.queryString);
      return typeof query.query === 'string';
    } catch {
      return false;
    }
  }

  public canPrettify(): boolean {
    return Boolean(this.isJsonLike() || this.parsedQuery);
  }

  public prettify(): WorkbenchQuery {
    const { queryString, parsedQuery } = this;
    if (parsedQuery) {
      return this.changeQueryString(parsedQuery.prettify().toString());
    } else {
      let parsedJson;
      try {
        parsedJson = Hjson.parse(queryString);
      } catch {
        return this;
      }
      return this.changeQueryString(JSONBig.stringify(parsedJson, undefined, 2));
    }
  }

  public isIngestQuery(): boolean {
    if (this.getEffectiveEngine() !== 'sql-msq-task') return false;

    const { queryString, parsedQuery } = this;
    if (parsedQuery) {
      return Boolean(parsedQuery.getIngestTable());
    }

    if (this.isJsonLike()) return false;

    return /(?:INSERT|REPLACE)\s+INTO/i.test(queryString);
  }

  public toggleUnlimited(): WorkbenchQuery {
    const { unlimited } = this;
    return this.changeUnlimited(!unlimited);
  }

  public makePreview(): WorkbenchQuery {
    if (!this.isIngestQuery()) return this;

    let ret: WorkbenchQuery = this;

    // Explicitly select MSQ, adjust the context, set maxNumTasks to the lowest possible and add in ingest mode flags
    const { queryContext } = this;
    ret = ret.changeEngine('sql-msq-task').changeQueryContext({
      ...queryContext,
      maxNumTasks: 2,
      finalizeAggregations: queryContext.finalizeAggregations ?? false,
      groupByEnableMultiValueUnnesting: queryContext.groupByEnableMultiValueUnnesting ?? false,
    });

    // Remove everything pertaining to INSERT INTO / REPLACE INTO from the query string
    const newQueryString = this.parsedQuery
      ? this.parsedQuery
          .changeInsertClause(undefined)
          .changeReplaceClause(undefined)
          .changePartitionedByClause(undefined)
          .changeClusteredByClause(undefined)
          .toString()
      : WorkbenchQuery.commentOutIngestParts(this.getQueryString());

    return ret.changeQueryString(newQueryString);
  }

  public setMaxNumTasksIfUnset(maxNumTasks: number | undefined): WorkbenchQuery {
    const { queryContext } = this;
    if (typeof queryContext.maxNumTasks === 'number' || !maxNumTasks) return this;

    return this.changeQueryContext({ ...queryContext, maxNumTasks: Math.max(maxNumTasks, 2) });
  }

  public getApiQuery(makeQueryId: () => string = uuidv4): {
    engine: DruidEngine;
    query: Record<string, any>;
    prefixLines: number;
    cancelQueryId?: string;
  } {
    const { queryString, queryContext, queryParameters, unlimited, prefixLines } = this;
    const engine = this.getEffectiveEngine();

    if (engine === 'native') {
      let query: any;
      try {
        query = Hjson.parse(queryString);
      } catch (e) {
        throw new Error(
          `You have selected the 'native' engine but the query you entered could not be parsed as JSON: ${e.message}`,
        );
      }
      query.context = { ...(query.context || {}), ...queryContext };

      let cancelQueryId = query.context.queryId;
      if (!cancelQueryId) {
        // If the queryId (sqlQueryId) is not explicitly set on the context generate one so it is possible to cancel the query.
        query.context.queryId = cancelQueryId = makeQueryId();
      }

      return {
        engine,
        query,
        prefixLines: prefixLines || 0,
        cancelQueryId,
      };
    }

    let apiQuery: Record<string, any> = {};
    if (this.isJsonLike()) {
      try {
        apiQuery = Hjson.parse(queryString);
      } catch (e) {
        throw new Error(`The query you entered could not be parsed as JSON: ${e.message}`);
      }
    } else {
      apiQuery = {
        query: queryString,
        resultFormat: 'array',
        header: true,
        typesHeader: true,
        sqlTypesHeader: true,
      };
    }

    const issueIndex = String(apiQuery.query).indexOf(ISSUE_MARKER);
    if (issueIndex !== -1) {
      const issueComment = String(apiQuery.query)
        .slice(issueIndex + ISSUE_MARKER.length)
        .split('\n')[0];
      throw new Error(
        `This query contains an ISSUE comment: ${issueComment
          .trim()
          .replace(
            /\.$/,
            '',
          )}. (Please resolve the issue in the comment, delete the ISSUE comment and re-run the query.)`,
      );
    }

    const ingestQuery = this.isIngestQuery();
    if (!unlimited && !ingestQuery && queryContext.selectDestination !== 'durableStorage') {
      apiQuery.context ||= {};
      apiQuery.context.sqlOuterLimit = 1001;
    }

    apiQuery.context = {
      ...(apiQuery.context || {}),
      ...queryContext,
    };

    let cancelQueryId: string | undefined;
    if (engine === 'sql-native') {
      cancelQueryId = apiQuery.context.sqlQueryId;
      if (!cancelQueryId) {
        // If the sqlQueryId is not explicitly set on the context generate one, so it is possible to cancel the query.
        apiQuery.context.sqlQueryId = cancelQueryId = makeQueryId();
      }
    }

    if (engine === 'sql-msq-task') {
      apiQuery.context.executionMode ??= 'async';
      if (ingestQuery) {
        // Alter these defaults for ingest queries if unset
        apiQuery.context.finalizeAggregations ??= false;
        apiQuery.context.groupByEnableMultiValueUnnesting ??= false;
        apiQuery.context.waitUntilSegmentsLoad ??= true;
      }
    }

    if (engine === 'sql-native' || engine === 'sql-msq-task') {
      apiQuery.context.sqlStringifyArrays ??= false;
    }

    if (Array.isArray(queryParameters) && queryParameters.length) {
      apiQuery.parameters = queryParameters;
    }

    return {
      engine,
      query: apiQuery,
      prefixLines: prefixLines || 0,
      cancelQueryId,
    };
  }
}
