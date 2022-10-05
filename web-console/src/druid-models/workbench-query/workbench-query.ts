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

import {
  SqlClusteredByClause,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlOrderByClause,
  SqlOrderByExpression,
  SqlPartitionedByClause,
  SqlQuery,
  SqlRef,
  SqlTableRef,
} from 'druid-query-toolkit';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';
import { v4 as uuidv4 } from 'uuid';

import { ColumnMetadata, deleteKeys, generate8HexId } from '../../utils';
import { DruidEngine, validDruidEngine } from '../druid-engine/druid-engine';
import { LastExecution } from '../execution/execution';
import { ExternalConfig } from '../external-config/external-config';
import {
  externalConfigToIngestQueryPattern,
  ingestQueryPatternToQuery,
} from '../ingest-query-pattern/ingest-query-pattern';
import { QueryContext } from '../query-context/query-context';

import { WorkbenchQueryPart } from './workbench-query-part';

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
  queryParts: WorkbenchQueryPart[];
  queryContext: QueryContext;
  engine?: DruidEngine;
  unlimited?: boolean;
}

export class WorkbenchQuery {
  static INLINE_DATASOURCE_MARKER = '__query_select';

  private static enabledQueryEngines: DruidEngine[] = ['native', 'sql-native'];

  static blank(): WorkbenchQuery {
    return new WorkbenchQuery({
      queryContext: {},
      queryParts: [WorkbenchQueryPart.blank()],
    });
  }

  static fromInitExternalConfig(
    externalConfig: ExternalConfig,
    isArrays: boolean[],
    timeExpression: SqlExpression | undefined,
  ): WorkbenchQuery {
    return new WorkbenchQuery({
      queryContext: {},
      queryParts: [
        WorkbenchQueryPart.fromQueryString(
          ingestQueryPatternToQuery(
            externalConfigToIngestQueryPattern(externalConfig, isArrays, timeExpression),
          ).toString(),
        ),
      ],
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

    const queryParts: WorkbenchQueryPart[] = [];
    let queryContext: QueryContext = {};
    for (let i = 0; i < headers.length; i++) {
      const header = headers[i];
      const body = bodies[i];
      if (header === 'Context') {
        queryContext = JSONBig.parse(body);
      } else if (header.startsWith('Helper:')) {
        queryParts.push(
          new WorkbenchQueryPart({
            id: generate8HexId(),
            queryName: header.replace(/^Helper:/, '').trim(),
            queryString: body,
            collapsed: true,
          }),
        );
      } else {
        queryParts.push(
          new WorkbenchQueryPart({
            id: generate8HexId(),
            queryString: body,
          }),
        );
      }
    }

    if (!queryParts.length) {
      queryParts.push(WorkbenchQueryPart.blank());
    }

    return new WorkbenchQuery({
      queryContext,
      queryParts,
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

    if (noSqlOuterLimit && !retQuery.getIngestDatasource()) {
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
          (_, spaces, thing) => `${spaces}--${thing.substr(2)}`,
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
        partitionedByExpression = SqlFunction.floor(
          SqlRef.column('__time'),
          partitionedByExpression,
        );
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

  public readonly queryParts: WorkbenchQueryPart[];
  public readonly queryContext: QueryContext;
  public readonly engine?: DruidEngine;
  public readonly unlimited?: boolean;

  constructor(value: WorkbenchQueryValue) {
    let queryParts = value.queryParts;
    if (!Array.isArray(queryParts) || !queryParts.length) {
      queryParts = [WorkbenchQueryPart.blank()];
    }
    if (!(queryParts instanceof WorkbenchQueryPart)) {
      queryParts = queryParts.map(p => new WorkbenchQueryPart(p));
    }
    this.queryParts = queryParts;
    this.queryContext = value.queryContext;

    // Start back compat code for the engine names that might be coming from local storage
    let possibleEngine: string | undefined = value.engine;
    if (possibleEngine === 'sql') {
      possibleEngine = 'sql-native';
    } else if (possibleEngine === 'sql-task') {
      possibleEngine = 'sql-msq-task';
    }
    // End bac compat code

    this.engine = validDruidEngine(possibleEngine) ? possibleEngine : undefined;

    if (value.unlimited) this.unlimited = true;
  }

  public valueOf(): WorkbenchQueryValue {
    return {
      queryParts: this.queryParts,
      queryContext: this.queryContext,
      engine: this.engine,
      unlimited: this.unlimited,
    };
  }

  public toString(): string {
    const { queryParts, queryContext } = this;
    return queryParts
      .slice(0, queryParts.length - 1)
      .flatMap(part => [`===== Helper: ${part.queryName} =====`, part.queryString])
      .concat([
        `===== Query =====`,
        this.getLastPart().queryString,
        `===== Context =====`,
        JSONBig.stringify(queryContext, undefined, 2),
      ])
      .join('\n\n');
  }

  public changeQueryParts(queryParts: WorkbenchQueryPart[]): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryParts });
  }

  public changeQueryContext(queryContext: QueryContext): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), queryContext });
  }

  public changeEngine(engine: DruidEngine | undefined): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), engine });
  }

  public changeUnlimited(unlimited: boolean): WorkbenchQuery {
    return new WorkbenchQuery({ ...this.valueOf(), unlimited });
  }

  public isTaskEngineNeeded(): boolean {
    return this.queryParts.some(part => part.isTaskEngineNeeded());
  }

  public getEffectiveEngine(): DruidEngine {
    const { engine } = this;
    if (engine) return engine;
    const enabledEngines = WorkbenchQuery.getQueryEngines();
    if (this.getLastPart().isJsonLike()) {
      if (this.getLastPart().isSqlInJson()) {
        if (enabledEngines.includes('sql-native')) return 'sql-native';
      } else {
        if (enabledEngines.includes('native')) return 'native';
      }
    }
    if (enabledEngines.includes('sql-msq-task') && this.isTaskEngineNeeded()) return 'sql-msq-task';
    if (enabledEngines.includes('sql-native')) return 'sql-native';
    return enabledEngines[0] || 'sql-native';
  }

  private getLastPart(): WorkbenchQueryPart {
    const { queryParts } = this;
    return queryParts[queryParts.length - 1];
  }

  public getId(): string {
    return this.getLastPart().id;
  }

  public getIds(): string[] {
    return this.queryParts.map(queryPart => queryPart.id);
  }

  public getQueryName(): string {
    return this.getLastPart().queryName || '';
  }

  public getQueryString(): string {
    return this.getLastPart().queryString;
  }

  public getCollapsed(): boolean {
    return this.getLastPart().collapsed;
  }

  public getLastExecution(): LastExecution | undefined {
    return this.getLastPart().lastExecution;
  }

  public getParsedQuery(): SqlQuery | undefined {
    return this.getLastPart().parsedQuery;
  }

  public isEmptyQuery(): boolean {
    return this.getLastPart().isEmptyQuery();
  }

  public isValid(): boolean {
    const lastPart = this.getLastPart();
    if (lastPart.isJsonLike() && !lastPart.validJson()) {
      return false;
    }

    return true;
  }

  public canPrettify(): boolean {
    const lastPart = this.getLastPart();
    return lastPart.isJsonLike();
  }

  public prettify(): WorkbenchQuery {
    const lastPart = this.getLastPart();
    let parsed;
    try {
      parsed = Hjson.parse(lastPart.queryString);
    } catch {
      return this;
    }
    return this.changeQueryString(JSONBig.stringify(parsed, undefined, 2));
  }

  public getIngestDatasource(): string | undefined {
    if (this.getEffectiveEngine() !== 'sql-msq-task') return;
    return this.getLastPart().getIngestDatasource();
  }

  public isIngestQuery(): boolean {
    return Boolean(this.getIngestDatasource());
  }

  private changeLastQueryPart(lastQueryPart: WorkbenchQueryPart): WorkbenchQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.slice(0, queryParts.length - 1).concat(lastQueryPart));
  }

  public changeQueryName(queryName: string): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeQueryName(queryName));
  }

  public changeQueryString(queryString: string): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeQueryString(queryString));
  }

  public changeCollapsed(collapsed: boolean): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeCollapsed(collapsed));
  }

  public changeLastExecution(lastExecution: LastExecution | undefined): WorkbenchQuery {
    return this.changeLastQueryPart(this.getLastPart().changeLastExecution(lastExecution));
  }

  public clear(): WorkbenchQuery {
    return new WorkbenchQuery({
      queryParts: [],
      queryContext: {},
    });
  }

  public toggleUnlimited(): WorkbenchQuery {
    const { unlimited } = this;
    return this.changeUnlimited(!unlimited);
  }

  public hasHelperQueries(): boolean {
    return this.queryParts.length > 1;
  }

  public materializeHelpers(): WorkbenchQuery {
    if (!this.hasHelperQueries()) return this;
    const { query } = this.getApiQuery();
    const queryString = query.query;
    if (typeof queryString !== 'string') return this;
    const lastPart = this.getLastPart();
    return this.changeQueryParts([
      new WorkbenchQueryPart({
        id: lastPart.id,
        queryName: lastPart.queryName,
        queryString,
      }),
    ]);
  }

  public extractCteHelpers(): WorkbenchQuery {
    const { queryParts } = this;

    let changed = false;
    const newParts = queryParts.flatMap(queryPart => {
      const helpers = queryPart.extractCteHelpers();
      if (helpers) changed = true;
      return helpers || [queryPart];
    });
    return changed ? this.changeQueryParts(newParts) : this;
  }

  public makePreview(): WorkbenchQuery {
    if (!this.isIngestQuery()) return this;

    let ret: WorkbenchQuery = this;

    // Limit all the helper queries
    const parsedQuery = this.getParsedQuery();
    if (parsedQuery) {
      const fromExpression = parsedQuery.getFirstFromExpression();
      if (fromExpression instanceof SqlTableRef) {
        const firstTable = fromExpression.getTable();
        ret = ret.changeQueryParts(
          this.queryParts.map(queryPart =>
            queryPart.queryName === firstTable ? queryPart.addPreviewLimit() : queryPart,
          ),
        );
      }
    }

    // Adjust the context, remove maxNumTasks and add in ingest mode flags
    const cleanContext = deleteKeys(this.queryContext, ['maxNumTasks']);
    ret = ret.changeQueryContext({
      ...cleanContext,
      finalizeAggregations: false,
      groupByEnableMultiValueUnnesting: false,
    });

    // Remove everything pertaining to INSERT INTO / REPLACE INTO from the query string
    const newQueryString = parsedQuery
      ? parsedQuery
          .changeInsertClause(undefined)
          .changeReplaceClause(undefined)
          .changePartitionedByClause(undefined)
          .changeClusteredByClause(undefined)
          .changeOrderByClause(
            WorkbenchQuery.makeOrderByClause(
              parsedQuery.partitionedByClause,
              parsedQuery.clusteredByClause,
            ),
          )
          .toString()
      : WorkbenchQuery.commentOutIngestParts(this.getQueryString());

    return ret.changeQueryString(newQueryString);
  }

  public getApiQuery(makeQueryId: () => string = uuidv4): {
    engine: DruidEngine;
    query: Record<string, any>;
    sqlPrefixLines?: number;
    cancelQueryId?: string;
  } {
    const { queryParts, queryContext, unlimited } = this;
    if (!queryParts.length) throw new Error(`should not get here`);
    const engine = this.getEffectiveEngine();

    const lastQueryPart = this.getLastPart();
    if (engine === 'native') {
      let query: any;
      try {
        query = Hjson.parse(lastQueryPart.queryString);
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
        cancelQueryId,
      };
    }

    const prefixParts = queryParts
      .slice(0, queryParts.length - 1)
      .filter(part => !part.getIngestDatasource());

    let apiQuery: Record<string, any> = {};
    if (lastQueryPart.isJsonLike()) {
      try {
        apiQuery = Hjson.parse(lastQueryPart.queryString);
      } catch (e) {
        throw new Error(`The query you entered could not be parsed as JSON: ${e.message}`);
      }
    } else {
      apiQuery = {
        query: lastQueryPart.queryString,
        resultFormat: 'array',
        header: true,
        typesHeader: true,
        sqlTypesHeader: true,
      };
    }

    let queryPrepend = '';
    let queryAppend = '';

    if (prefixParts.length) {
      const { insertReplaceLine, overwriteLine, partitionedByLine, clusteredByLine } =
        WorkbenchQuery.getIngestionLines(apiQuery.query);
      if (insertReplaceLine) {
        queryPrepend += insertReplaceLine + '\n';
        if (overwriteLine) {
          queryPrepend += overwriteLine + '\n';
        }

        apiQuery.query = WorkbenchQuery.commentOutIngestParts(apiQuery.query);

        if (clusteredByLine) {
          queryAppend = '\n' + clusteredByLine + queryAppend;
        }
        if (partitionedByLine) {
          queryAppend = '\n' + partitionedByLine + queryAppend;
        }
      }

      queryPrepend += 'WITH\n' + prefixParts.map(p => p.toWithPart()).join(',\n') + '\n(\n';
      queryAppend = '\n)' + queryAppend;
    }

    let prefixLines = 0;
    if (queryPrepend) {
      prefixLines = queryPrepend.split('\n').length - 1;
      apiQuery.query = queryPrepend + apiQuery.query + queryAppend;
    }

    const m = /--:ISSUE:(.+)(?:\n|$)/.exec(apiQuery.query);
    if (m) {
      throw new Error(
        `This query contains an ISSUE comment: ${m[1]
          .trim()
          .replace(
            /\.$/,
            '',
          )}. (Please resolve the issue in the comment, delete the ISSUE comment and re-run the query.)`,
      );
    }

    const ingestQuery = this.isIngestQuery();
    if (!unlimited && !ingestQuery) {
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
      apiQuery.context.finalizeAggregations ??= !ingestQuery;
      apiQuery.context.groupByEnableMultiValueUnnesting ??= !ingestQuery;
    }

    return {
      engine,
      query: apiQuery,
      sqlPrefixLines: prefixLines,
      cancelQueryId,
    };
  }

  public getInlineMetadata(): ColumnMetadata[] {
    const { queryParts } = this;
    if (!queryParts.length) return [];
    return queryParts.slice(0, queryParts.length - 1).flatMap(p => p.getInlineMetadata());
  }

  public getPrefix(index: number): WorkbenchQuery {
    return this.changeQueryParts(this.queryParts.slice(0, index + 1));
  }

  public getPrefixQueries(): WorkbenchQuery[] {
    return this.queryParts.slice(0, this.queryParts.length - 1).map((_, i) => this.getPrefix(i));
  }

  public applyUpdate(newQuery: WorkbenchQuery, index: number): WorkbenchQuery {
    return newQuery.changeQueryParts(newQuery.queryParts.concat(this.queryParts.slice(index + 1)));
  }

  public duplicate(): WorkbenchQuery {
    return this.changeQueryParts(this.queryParts.map(part => part.duplicate()));
  }

  public duplicateLast(): WorkbenchQuery {
    const { queryParts } = this;
    const last = this.getLastPart();
    return this.changeQueryParts(queryParts.concat(last.duplicate()));
  }

  public addBlank(): WorkbenchQuery {
    const { queryParts } = this;
    const last = this.getLastPart();
    return this.changeQueryParts(
      queryParts.slice(0, queryParts.length - 1).concat(
        last
          .changeQueryName(last.queryName || 'q')
          .changeCollapsed(true)
          .changeLastExecution(undefined),
        WorkbenchQueryPart.blank(),
      ),
    );
  }

  public remove(index: number): WorkbenchQuery {
    const { queryParts } = this;
    return this.changeQueryParts(queryParts.filter((_, i) => i !== index));
  }
}
