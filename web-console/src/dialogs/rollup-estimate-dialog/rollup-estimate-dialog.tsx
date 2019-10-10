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

import { Dialog } from '@blueprintjs/core';
import axios from 'axios';
import {
  HeaderRows,
  isFirstRowHeader,
  normalizeQueryResult,
  shouldIncludeTimestamp,
  sqlParserFactory,
  SqlQuery,
} from 'druid-query-toolkit';
import Hjson from 'hjson';
import memoizeOne from 'memoize-one';
import * as React from 'react';
import ReactTable from 'react-table';

import { SQL_FUNCTIONS } from '../../../lib/sql-docs';
import { Loader } from '../../components/index';
import { getDruidErrorMessage, QueryManager } from '../../utils/index';
import { isEmptyContext, QueryContext } from '../../utils/query-context';
import { QueryView } from '../../views/index';
import { QueryExtraInfoData } from '../../views/query-view/query-extra-info/query-extra-info';

import { RollupRatio } from './rollup-ratio';

import './rollup-estimate-dialog.scss';

const parserRaw = sqlParserFactory(SQL_FUNCTIONS.map(sqlFunction => sqlFunction.name));

const parser = memoizeOne((sql: string) => {
  try {
    return parserRaw(sql);
  } catch {
    return;
  }
});

interface QueryWithContext {
  queryString: string;
  queryContext: QueryContext;
  wrapQueryLimit: number | undefined;
}

interface QueryResult {
  queryResult: HeaderRows;
  queryExtraInfo: QueryExtraInfoData;
  parsedQuery?: SqlQuery;
}

export interface RollupEstimateDialogProps {
  datasource: string;
  onClose: () => void;
}

export interface RollupEstimateDialogState {
  queryString: string;
  queryContext: QueryContext;
  queryColumns?: string[];
  wrapQueryLimit: number | undefined;
  loading: boolean;
  result?: QueryResult;
  error?: string;
}

export class RollupEstimateDialog extends React.PureComponent<
  RollupEstimateDialogProps,
  RollupEstimateDialogState
> {
  private sqlQueryManager: QueryManager<QueryWithContext, QueryResult>;

  constructor(props: RollupEstimateDialogProps, context: any) {
    super(props, context);
    this.state = {
      // Rename some of these variables
      queryString: `SELECT * FROM "${props.datasource}"`,
      queryContext: {},
      queryColumns: [],
      wrapQueryLimit: 20,
      loading: false,
    };

    this.sqlQueryManager = new QueryManager({
      processQuery: async (queryWithContext: QueryWithContext): Promise<QueryResult> => {
        const { queryString, queryContext, wrapQueryLimit } = queryWithContext;
        let parsedQuery: SqlQuery | undefined;
        let jsonQuery: any;
        // Note: Might need to clean this function up and delete some imports
        try {
          parsedQuery = parser(queryString);
        } catch {}

        if (!(parsedQuery instanceof SqlQuery)) {
          parsedQuery = undefined;
        }
        if (QueryView.isJsonLike(queryString)) {
          jsonQuery = Hjson.parse(queryString);
        } else {
          const actualQuery = QueryView.wrapInLimitIfNeeded(queryString, wrapQueryLimit);

          jsonQuery = {
            query: actualQuery,
            resultFormat: 'array',
            header: true,
          };
        }

        if (!isEmptyContext(queryContext)) {
          jsonQuery.context = Object.assign(jsonQuery.context || {}, queryContext);
        }

        let rawQueryResult: unknown;
        let queryId: string | undefined;
        let sqlQueryId: string | undefined;
        const startTime = new Date();
        let endTime: Date;
        if (!jsonQuery.queryType && typeof jsonQuery.query === 'string') {
          try {
            const sqlResultResp = await axios.post('/druid/v2/sql', jsonQuery);
            endTime = new Date();
            rawQueryResult = sqlResultResp.data;
            sqlQueryId = sqlResultResp.headers['x-druid-sql-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }
        } else {
          try {
            const runeResultResp = await axios.post('/druid/v2', jsonQuery);
            endTime = new Date();
            rawQueryResult = runeResultResp.data;
            queryId = runeResultResp.headers['x-druid-query-id'];
          } catch (e) {
            throw new Error(getDruidErrorMessage(e));
          }
        }

        const queryResult = normalizeQueryResult(
          rawQueryResult,
          shouldIncludeTimestamp(jsonQuery),
          isFirstRowHeader(jsonQuery),
        );

        console.log(queryResult);
        return {
          queryResult,
          queryExtraInfo: {
            queryId,
            sqlQueryId,
            startTime,
            endTime,
            numResults: queryResult.rows.length,
            wrapQueryLimit,
          },
          parsedQuery,
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result,
          loading,
          error,
          queryColumns: result ? result.queryResult.header : [],
        });
      },
    });
  }

  componentDidMount() {
    // Do something when component unmounts
    const { queryString, queryContext, wrapQueryLimit } = this.state;
    this.sqlQueryManager.runQuery({ queryString, queryContext, wrapQueryLimit });
  }
  render(): JSX.Element {
    // Plan to separate main and control components
    const { datasource, onClose } = this.props;
    const { result, loading, queryColumns } = this.state;
    console.log(result);
    console.log(queryColumns);
    if (loading) return <Loader />;
    return (
      <Dialog
        className="rollup-estimate-dialog"
        isOpen={datasource !== undefined}
        onClose={onClose}
        canOutsideClickClose={false}
        title={'Estimate your rollup ratio'}
      >
        <ReactTable
          data={result ? result.queryResult.rows : []}
          loading={loading}
          columns={(result ? result.queryResult.header : []).map((h: any, i) => {
            // Need to clean this up and clickable columns for deselect, will have to update queryColumns too
            return {
              Header: () => {
                return <div>{h}</div>;
              },
              headerClassName: h,
              accessor: String(i),
              Cell: row => {
                const value = row.value;
                const popover = (
                  <div>
                    <div>{value}</div>
                  </div>
                );
                if (value) {
                  return popover;
                }
                return value;
              },
            };
          })}
          showPageSizeOptions={false}
          showPagination={false}
        />
        <RollupRatio datasource={datasource} queryColumns={queryColumns ? queryColumns : []} />
      </Dialog>
    );
  }
}
