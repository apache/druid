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
import { HeaderRows, normalizeQueryResult } from 'druid-query-toolkit';
import * as React from 'react';

import { Loader } from '../../components/index';
import { getDruidErrorMessage, queryDruidRune, QueryManager } from '../../utils/index';
import { QueryContext } from '../../utils/query-context';

import './rollup-ratio.scss';

export interface RollupRatioProps {
  queryColumns: string[];
  datasource: string;
}

export interface RollupRatioState {
  rollupQueryString: string;
  result?: HeaderRows;
  queryContext: QueryContext;
  loading: boolean;
  error?: string;
}

export class RollupRatio extends React.PureComponent<RollupRatioProps, RollupRatioState> {
  private druidQueryManager: QueryManager<null, HeaderRows>;
  constructor(props: RollupRatioProps, context: any) {
    super(props, context);
    this.state = {
      queryContext: {},
      rollupQueryString: `
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "${this.props.datasource}"
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "count",
      "name": "a0"
    },
    {
      "type": "cardinality",
      "name": "a1",
      "fields": [
        ${this.props.queryColumns.map(column => `"${column}"`)}
      ],
      "byRow": true
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
    "sqlQueryId": "37382db2-d30f-43a0-86ec-49dbc48b97b8"
  }
}
      `,
      // rollupQueryString: `SELECT COUNT("${this.props.queryColumns[1]}") / COUNT(DISTINCT "${this.props.queryColumns[1]}") * 1.0 FROM "${this.props.datasource}"`,
      loading: false,
    };
    this.druidQueryManager = new QueryManager({
      processQuery: async (): Promise<HeaderRows> => {
        const { datasource, queryColumns } = this.props;
        let rawQueryResult: any;
        try {
          const timeseriesResponse = await queryDruidRune({
            queryType: 'timeseries',
            dataSource: datasource,
            intervals: ['2013-01-01/2020-01-01'],
            descending: false,
            filter: null,
            granularity: { type: 'all' },
            aggregations: [
              { type: 'count', name: 'a0' },
              {
                type: 'cardinality',
                name: 'a1',
                fields: queryColumns,
                byRow: true,
              },
            ],
            postAggregations: [
              {
                type: 'expression',
                name: 'p0',
                expression: '(("a0" / "a1") * 1.0)',
                ordering: null,
              },
            ],
            limit: 10,
            context: { skipEmptyBuckets: true, sqlQueryId: '37382db2-d30f-43a0-86ec-49dbc48b97b8' },
          });
          console.log(timeseriesResponse);

          if (Array.isArray(timeseriesResponse) && timeseriesResponse.length === 1) {
            rawQueryResult = timeseriesResponse;
            console.log(rawQueryResult);
          } else {
            throw new Error(`unexpected response from segmentMetadata query`);
          }
        } catch (e) {
          throw new Error(getDruidErrorMessage(e));
        }

        const queryResult = normalizeQueryResult(rawQueryResult);

        console.log(queryResult);
        return queryResult;
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
    this.druidQueryManager.runQuery(null);
  }

  componentDidUpdate(prevProps: RollupRatioProps) {
    const { queryColumns, datasource } = this.props;
    if (prevProps.queryColumns !== queryColumns) {
      this.setState(
        {
          rollupQueryString: `
{
  "queryType": "timeseries",
  "dataSource": {
    "type": "table",
    "name": "${datasource}"
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "descending": false,
  "filter": null,
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {
      "type": "count",
      "name": "a0"
    },
    {
      "type": "cardinality",
      "name": "a1",
      "fields": [
        ${queryColumns.map(column => `"${column}"`)}
      ],
      "byRow": true
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
    "sqlQueryId": "37382db2-d30f-43a0-86ec-49dbc48b97b8"
  }
}    
`,
        },
        () => {
          this.druidQueryManager.runQuery(null);
        },
      );
    }
  }

  render(): JSX.Element {
    const { loading, result } = this.state;
    // const { queryColumns } = this.props;
    // console.log(queryColumns);
    if (loading) return <Loader />;

    // console.log(rollupQueryString);
    return (
      <div className="rollup-ratio">
        <Callout>
          <p>
            You may select any column to exclude them from your rollup preview. This will update
            your rollup ratio.{' '}
          </p>
          <p>Your rollup ratio is currently: {result ? result.rows[0][1] : []}</p>
        </Callout>
      </div>
    );
  }
}
