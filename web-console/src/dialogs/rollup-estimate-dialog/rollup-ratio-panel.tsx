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

import { Button, Callout, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import { HeaderRows, normalizeQueryResult } from 'druid-query-toolkit';
import * as React from 'react';

// import { Loader } from '../../components/index';
import { getDruidErrorMessage, queryDruidRune, QueryManager } from '../../utils/index';

import './rollup-ratio.scss';

export interface RollupRatioPanelProps {
  queryColumns: string[];
  rollupRatio: number;
  datasource: string;
  interval: string;
  updateInterval: (interval: string) => void;
}

export interface RollupRatioPanelState {
  result?: HeaderRows;
  loading: boolean;
  error?: string;
  intervalInput: string;
}

export class RollupRatioPanel extends React.PureComponent<
  RollupRatioPanelProps,
  RollupRatioPanelState
> {
  private druidQueryManager: QueryManager<null, HeaderRows>;
  constructor(props: RollupRatioPanelProps, context: any) {
    super(props, context);
    this.state = {
      loading: false,
      intervalInput: this.props.interval,
    };
    this.druidQueryManager = new QueryManager({
      processQuery: async (): Promise<HeaderRows> => {
        const { datasource, queryColumns, interval } = this.props;
        let rawQueryResult: any;
        try {
          const timeseriesResponse = await queryDruidRune({
            queryType: 'timeseries',
            dataSource: datasource,
            intervals: [interval],
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

  // componentDidUpdate(prevProps: RollupRatioPanelProps) {
  //   const { queryColumns } = this.props;
  //   if (prevProps.queryColumns !== queryColumns) {
  //     this.druidQueryManager.runQuery(null);
  //   }
  // }

  render(): JSX.Element {
    const { intervalInput, result } = this.state;
    const { rollupRatio, updateInterval } = this.props;
    // if (loading) return <Loader />;
    return (
      <>
        <div className="rollup-ratio">
          <Callout>
            <p>
              You may select any column to exclude them from your rollup preview. This will update
              your rollup ratio after you click on "Estimate Rollup".{' '}
            </p>
            <p>Please click on "Preview Data" after modifying your interval.</p>
            <p>
              {rollupRatio !== -1
                ? `This datasource has previously been rolled up. The original rollup ratio is ${(rollupRatio -
                    1) *
                    100}%`
                : ''}
            </p>
          </Callout>

          <FormGroup label={`Your current rollup ratio:`}>
            <InputGroup
              value={result ? ((Math.max(result.rows[0][1], 1) - 1) * 100).toFixed(2) + '%' : ''}
              readOnly
            />
          </FormGroup>
          <FormGroup
            // key={field.name}
            label={`Interval`}
          >
            <InputGroup
              value={intervalInput}
              placeholder="2019-01-01/2020-01-01"
              onChange={(e: any) => {
                this.setState({ intervalInput: e.target.value });
                console.log(intervalInput);
              }}
            />
          </FormGroup>

          <Button
            text="Preview data"
            onClick={() => {
              updateInterval(intervalInput);
            }}
          />
        </div>
        <div className="rollup-ratio-submit">
          <Button
            text="Estimate rollup"
            intent={Intent.PRIMARY}
            onClick={() => {
              this.druidQueryManager.runQuery(null);
            }}
          />
        </div>
      </>
    );
  }
}
