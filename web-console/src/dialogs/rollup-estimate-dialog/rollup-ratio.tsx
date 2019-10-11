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

import { Callout } from '@blueprintjs/core';
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

import { SQL_FUNCTIONS } from '../../../lib/sql-docs';
import { Loader } from '../../components/index';
import { getDruidErrorMessage, QueryManager } from '../../utils/index';
import { isEmptyContext, QueryContext } from '../../utils/query-context';
import { QueryView } from '../../views/index';
import { QueryExtraInfoData } from '../../views/query-view/query-extra-info/query-extra-info';

import './rollup-ratio.scss';

const parserRaw = sqlParserFactory(SQL_FUNCTIONS.map(sqlFunction => sqlFunction.name));

const parser = memoizeOne((sql: string) => {
  try {
    return parserRaw(sql);
  } catch {
    return;
  }
});

interface QueryWithContext {
  rollupQueryString: string;
  queryContext: QueryContext;
  wrapQueryLimit: number | undefined;
}

interface QueryResult {
  queryResult: HeaderRows;
  queryExtraInfo: QueryExtraInfoData;
  parsedQuery?: SqlQuery;
}

export interface RollupRatioProps {
  queryColumns: string[];
  datasource: string;
}

export interface RollupRatioState {
  rollupQueryString: string;
  result?: QueryResult;
  queryContext: QueryContext;
  loading: boolean;
  error?: string;
}

export class RollupRatio extends React.PureComponent<RollupRatioProps, RollupRatioState> {
  private sqlQueryManager: QueryManager<QueryWithContext, QueryResult>;
  constructor(props: RollupRatioProps, context: any) {
    super(props, context);
    this.state = {
      queryContext: {},
      rollupQueryString: `
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "${props.datasource}"
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "virtualColumns": [
    {
      "type": "expression",
      "name": "v0",
      "expression": "concat(${props.queryColumns})",
      "outputType": "STRING"
    }
  ],
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "filtered",
      "aggregator": {
        "type": "count",
        "name": "a0"
      },
      "filter": {
        "type": "not",
        "field": {
          "type": "selector",
          "dimension": "v0",
          "value": null,
          "extractionFn": null
        }
      },
      "name": "a0"
    },
    {
      "type": "cardinality",
      "name": "a1",
      "fields": [
        {
          "type": "default",
          "dimension": "v0",
          "outputName": "v0",
          "outputType": "STRING"
        }
      ],
      "byRow": true,
      "round": true
    }
  ],
  "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "((\\"a0\\" / \\"a1\\") * 1.0)",
      "ordering": null
    }
  ],
  "limit": 2147483647,
  "context": {
    "skipEmptyBuckets": true,
    "sqlQueryId": "5b534ba2-be62-4e5a-bb6b-c3c4aef416bf"
  }
}
      `,
      // rollupQueryString: `SELECT COUNT("${this.props.queryColumns[1]}") / COUNT(DISTINCT "${this.props.queryColumns[1]}") * 1.0 FROM "${this.props.datasource}"`,
      loading: false,
    };
    this.sqlQueryManager = new QueryManager({
      // Clean up this function along with renaming some variables
      processQuery: async (queryWithContext: QueryWithContext): Promise<QueryResult> => {
        const { rollupQueryString, queryContext, wrapQueryLimit } = queryWithContext;
        console.log(this.state.rollupQueryString);

        let parsedQuery: SqlQuery | undefined;
        let jsonQuery: any;
        try {
          parsedQuery = parser(rollupQueryString);
        } catch {}

        if (!(parsedQuery instanceof SqlQuery)) {
          parsedQuery = undefined;
        }
        if (QueryView.isJsonLike(rollupQueryString)) {
          jsonQuery = Hjson.parse(rollupQueryString);
        } else {
          const actualQuery = QueryView.wrapInLimitIfNeeded(rollupQueryString, wrapQueryLimit);

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
        });
      },
    });
  }
  componentDidMount() {
    const { rollupQueryString, queryContext } = this.state;
    this.sqlQueryManager.runQuery({ rollupQueryString, queryContext, wrapQueryLimit: 1 });
  }

  componentDidUpdate(prevProps: RollupRatioProps) {
    const { queryColumns } = this.props;
    const { rollupQueryString, queryContext } = this.state;
    if (prevProps.queryColumns !== queryColumns) {
      this.setState({
        rollupQueryString: `
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": ${this.props.datasource}
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "virtualColumns": [
    {
      "type": "expression",
      "name": "v0",
      "expression": "concat(${this.props.queryColumns})",
      "outputType": "STRING"
    }
  ],
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "filtered",
      "aggregator": {
        "type": "count",
        "name": "a0"
      },
      "filter": {
        "type": "not",
        "field": {
          "type": "selector",
          "dimension": "v0",
          "value": null,
          "extractionFn": null
        }
      },
      "name": "a0"
    },
    {
      "type": "cardinality",
      "name": "a1",
      "fields": [
        {
          "type": "default",
          "dimension": "v0",
          "outputName": "v0",
          "outputType": "STRING"
        }
      ],
      "byRow": true,
      "round": true
    }
  ],
  "postAggregations": [
    {
      "type": "expression",
      "name": "p0",
      "expression": "((\\"a0\\" / \\"a1\\") * 1.0)",
      "ordering": null
    }
  ],
  "limit": 2147483647,
  "context": {
    "skipEmptyBuckets": true,
    "sqlQueryId": "5b534ba2-be62-4e5a-bb6b-c3c4aef416bf"
  }
} 
`,
      });
      this.sqlQueryManager.runQuery({ rollupQueryString, queryContext, wrapQueryLimit: 1 });
    }
  }

  render(): JSX.Element {
    const { loading, result } = this.state;
    // const { queryColumns } = this.props;
    // console.log(queryColumns);
    if (loading) return <Loader />;
    console.log(result);
    // console.log(rollupQueryString);
    return (
      <div className="rollup-ratio">
        <Callout>
          <p>
            You may select any column to exclude them from your rollup preview. This will update
            your rollup ratio.{' '}
          </p>
          <p>Your rollup ratio is currently: {result ? result.queryResult.rows[0][1] : []}</p>
        </Callout>
      </div>
    );
  }
}
