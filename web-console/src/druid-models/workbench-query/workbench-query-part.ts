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

import { SqlExpression, SqlQuery, SqlTableRef, SqlValues, SqlWithQuery } from 'druid-query-toolkit';
import Hjson from 'hjson';
import * as JSONBig from 'json-bigint-native';

import { ColumnMetadata, compact, filterMap, generate8HexId, sqlTypeFromDruid } from '../../utils';
import { LastExecution, validateLastExecution } from '../execution/execution';
import { fitExternalConfigPattern } from '../external-config/external-config';

// -----------------------------

export interface WorkbenchQueryPartValue {
  id: string;
  queryName?: string;
  queryString: string;
  collapsed?: boolean;
  lastExecution?: LastExecution;
}

export class WorkbenchQueryPart {
  static blank() {
    return new WorkbenchQueryPart({
      id: generate8HexId(),
      queryString: '',
    });
  }

  static fromQuery(query: SqlQuery | SqlValues, queryName?: string, collapsed?: boolean) {
    return this.fromQueryString(query.changeParens([]).toString(), queryName, collapsed);
  }

  static fromQueryString(queryString: string, queryName?: string, collapsed?: boolean) {
    return new WorkbenchQueryPart({
      id: generate8HexId(),
      queryName,
      queryString,
      collapsed,
    });
  }

  static isTaskEngineNeeded(queryString: string): boolean {
    return /EXTERN\s*\(|(?:INSERT|REPLACE)\s+INTO/im.test(queryString);
  }

  static getIngestDatasourceFromQueryFragment(queryFragment: string): string | undefined {
    // Assuming the queryFragment is no parsable find the prefix that look like:
    // REPLACE<space>INTO<space><whatever><space>SELECT<space or EOF>
    const matchInsertReplaceIndex = queryFragment.match(/(?:INSERT|REPLACE)\s+INTO/)?.index;
    if (typeof matchInsertReplaceIndex !== 'number') return;

    const matchEnd = queryFragment.match(/\b(?:SELECT|WITH)\b|$/);
    const fragmentQuery = SqlQuery.maybeParse(
      queryFragment.substring(matchInsertReplaceIndex, matchEnd?.index) + ' SELECT * FROM t',
    );
    if (!fragmentQuery) return;

    return fragmentQuery.getIngestTable()?.getTable();
  }

  public readonly id: string;
  public readonly queryName?: string;
  public readonly queryString: string;
  public readonly collapsed: boolean;
  public readonly lastExecution?: LastExecution;

  public readonly parsedQuery?: SqlQuery;

  constructor(value: WorkbenchQueryPartValue) {
    this.id = value.id;
    this.queryName = value.queryName;
    this.queryString = value.queryString;
    this.collapsed = Boolean(value.collapsed);
    this.lastExecution = validateLastExecution(value.lastExecution);

    try {
      this.parsedQuery = SqlQuery.parse(this.queryString);
    } catch {}
  }

  public valueOf(): WorkbenchQueryPartValue {
    return {
      id: this.id,
      queryName: this.queryName,
      queryString: this.queryString,
      collapsed: this.collapsed,
      lastExecution: this.lastExecution,
    };
  }

  public changeId(id: string): WorkbenchQueryPart {
    return new WorkbenchQueryPart({ ...this.valueOf(), id });
  }

  public changeQueryName(queryName: string): WorkbenchQueryPart {
    return new WorkbenchQueryPart({ ...this.valueOf(), queryName });
  }

  public changeQueryString(queryString: string): WorkbenchQueryPart {
    return new WorkbenchQueryPart({ ...this.valueOf(), queryString });
  }

  public changeCollapsed(collapsed: boolean): WorkbenchQueryPart {
    return new WorkbenchQueryPart({ ...this.valueOf(), collapsed });
  }

  public changeLastExecution(lastExecution: LastExecution | undefined): WorkbenchQueryPart {
    return new WorkbenchQueryPart({ ...this.valueOf(), lastExecution });
  }

  public clear(): WorkbenchQueryPart {
    return new WorkbenchQueryPart({
      ...this.valueOf(),
      queryString: '',
    });
  }

  public isEmptyQuery(): boolean {
    return this.queryString.trim() === '';
  }

  public isJsonLike(): boolean {
    return this.queryString.trim().startsWith('{');
  }

  public validJson(): boolean {
    try {
      Hjson.parse(this.queryString);
      return true;
    } catch {
      return false;
    }
  }

  public isSqlInJson(): boolean {
    try {
      const query = Hjson.parse(this.queryString);
      return typeof query.query === 'string';
    } catch {
      return false;
    }
  }

  public getSqlString(): string {
    if (this.isJsonLike()) {
      const query = Hjson.parse(this.queryString);
      return typeof query.query === 'string' ? query.query : '';
    } else {
      return this.queryString;
    }
  }

  public prettyPrintJson(): WorkbenchQueryPart {
    let parsed: unknown;
    try {
      parsed = Hjson.parse(this.queryString);
    } catch {
      return this;
    }
    return this.changeQueryString(JSONBig.stringify(parsed, undefined, 2));
  }

  public getIngestDatasource(): string | undefined {
    const { queryString, parsedQuery } = this;
    if (parsedQuery) {
      return parsedQuery.getIngestTable()?.getTable();
    }

    if (this.isJsonLike()) return;

    return WorkbenchQueryPart.getIngestDatasourceFromQueryFragment(queryString);
  }

  public getInlineMetadata(): ColumnMetadata[] {
    const { queryName, parsedQuery } = this;
    if (queryName && parsedQuery) {
      try {
        return fitExternalConfigPattern(parsedQuery).signature.map(({ name, type }) => ({
          COLUMN_NAME: name,
          DATA_TYPE: sqlTypeFromDruid(type),
          TABLE_NAME: queryName,
          TABLE_SCHEMA: 'druid',
        }));
      } catch {
        return filterMap(parsedQuery.getSelectExpressionsArray(), ex => {
          const outputName = ex.getOutputName();
          if (!outputName) return;
          return {
            COLUMN_NAME: outputName,
            DATA_TYPE: 'UNKNOWN',
            TABLE_NAME: queryName,
            TABLE_SCHEMA: 'druid',
          };
        });
      }
    }
    return [];
  }

  public isTaskEngineNeeded(): boolean {
    return WorkbenchQueryPart.isTaskEngineNeeded(this.queryString);
  }

  public extractCteHelpers(): WorkbenchQueryPart[] | undefined {
    let flatQuery: SqlQuery;
    try {
      // We need to do our own parsing here because this.parseQuery necessarily must be a SqlQuery
      // object, and we might have a SqlWithQuery here.
      flatQuery = (SqlExpression.parse(this.queryString) as SqlWithQuery).flattenWith();
    } catch {
      return;
    }

    const possibleNewParts = flatQuery.getWithParts().map(({ table, columns, query }) => {
      if (columns) return;
      return WorkbenchQueryPart.fromQuery(query, table.name, true);
    });
    if (!possibleNewParts.length) return;

    const newParts = compact(possibleNewParts);
    if (newParts.length !== possibleNewParts.length) return;

    return newParts.concat(this.changeQueryString(flatQuery.changeWithParts(undefined).toString()));
  }

  public toWithPart(): string {
    const { queryName, queryString } = this;
    return `${SqlTableRef.create(queryName || 'q')} AS (\n${queryString}\n)`;
  }

  public duplicate(): WorkbenchQueryPart {
    return this.changeId(generate8HexId()).changeLastExecution(undefined);
  }

  public addPreviewLimit(): WorkbenchQueryPart {
    const { parsedQuery } = this;
    if (!parsedQuery || parsedQuery.hasLimit()) return this;
    return this.changeQueryString(parsedQuery.changeLimitValue(10000).toString());
  }
}
