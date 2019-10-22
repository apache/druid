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
import { HeaderRows, normalizeQueryResult } from 'druid-query-toolkit';
import * as React from 'react';
import ReactTable from 'react-table';

import { Loader } from '../../components/index';
import { getDruidErrorMessage, queryDruidRune, QueryManager } from '../../utils/index';

import { RollupRatioPanel } from './rollup-ratio-panel';

import './rollup-estimate-dialog.scss';

const CURRENT_YEAR = new Date().getUTCFullYear();

interface QueryResult {
  queryResult: HeaderRows;
  rollupRatio: number;
}

export interface RollupEstimateDialogProps {
  datasource: string;
  onClose: () => void;
}

export interface RollupEstimateDialogState {
  queryColumns: string[];
  rollupRatio: number;
  interval: string;
  loading: boolean;
  result?: QueryResult;
  error?: string;
}

export class RollupEstimateDialog extends React.PureComponent<
  RollupEstimateDialogProps,
  RollupEstimateDialogState
> {
  private druidQueryManager: QueryManager<null, QueryResult>;

  constructor(props: RollupEstimateDialogProps, context: any) {
    super(props, context);
    this.state = {
      rollupRatio: -1,
      interval: `${CURRENT_YEAR - 10}-01-01/${CURRENT_YEAR + 1}-01-01`,
      queryColumns: [],
      loading: false,
    };

    this.druidQueryManager = new QueryManager({
      processQuery: async (): Promise<QueryResult> => {
        const { interval } = this.state;
        const { datasource } = this.props;
        let rawQueryResult: any;
        let rollupRatio: any = -1;
        try {
          const segmentMetadataResponse = await queryDruidRune({
            queryType: 'segmentMetadata',
            dataSource: datasource,
            intervals: [`${interval}`],
            merge: true,
            lenientAggregatorMerge: true,
            analysisTypes: ['timestampSpec', 'queryGranularity', 'aggregators', 'rollup'],
          });

          if (Array.isArray(segmentMetadataResponse) && segmentMetadataResponse.length === 1) {
            const segmentMetadataResponse0 = segmentMetadataResponse[0];
            if (segmentMetadataResponse0.rollup) {
              try {
                // Get rollup ratio of ingested data
                const rollupRatioResponse = await queryDruidRune({
                  queryType: 'timeseries',
                  dataSource: datasource,
                  intervals: [`${interval}`],
                  filter: null,
                  granularity: { type: 'all' },
                  aggregations: [
                    {
                      type: 'longSum',
                      name: 'a0',
                      fieldName: 'count',
                    },
                    {
                      type: 'count',
                      name: 'a1',
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
                });
                rollupRatio = rollupRatioResponse[0].result.p0;
              } catch (e) {
                throw new Error(getDruidErrorMessage(e));
              }
            }
            try {
              // Get preview of data source
              const previewDataResponse = await queryDruidRune({
                queryType: 'scan',
                dataSource: datasource,
                intervals: [`${interval}`],
                resultFormat: 'compactedList',
                limit: 20,
                columns: Object.keys(segmentMetadataResponse0.columns).filter(
                  (column: any) =>
                    !Object.keys(segmentMetadataResponse0.aggregators).includes(column),
                ),
              });
              rawQueryResult = previewDataResponse;
            } catch (e) {
              throw new Error(getDruidErrorMessage(e));
            }
          } else {
            throw new Error(`unexpected response from segmentMetadata query`);
          }
        } catch (e) {
          throw new Error(getDruidErrorMessage(e));
        }

        const queryResult = normalizeQueryResult(rawQueryResult);
        return { queryResult, rollupRatio };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result,
          loading,
          error,
          queryColumns: result ? result.queryResult.header : [],
          rollupRatio: result ? result.rollupRatio : -1,
        });
      },
    });
  }

  updateColumns(columnName: string) {
    // Include or exclude columns for rollup
    const { queryColumns } = this.state;

    const newQueryColumns = queryColumns.slice();
    if (!queryColumns.includes(columnName)) {
      newQueryColumns.push(columnName);
    } else if (queryColumns.length !== 1) {
      const indexOfColumn = newQueryColumns.indexOf(columnName);
      newQueryColumns.splice(indexOfColumn, 1);
    }
    this.setState({ queryColumns: newQueryColumns });
  }

  updateInterval(newInterval: string) {
    this.setState({ interval: newInterval }, () => {
      this.druidQueryManager.runQuery(null);
    });
  }

  componentDidMount(): void {
    this.druidQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.druidQueryManager.terminate();
  }

  render(): JSX.Element {
    const { datasource, onClose } = this.props;
    const { interval, result, loading, queryColumns, rollupRatio } = this.state;
    console.log(result);
    return (
      <Dialog
        className="rollup-estimate-dialog"
        isOpen={datasource !== undefined}
        onClose={onClose}
        canOutsideClickClose={false}
        title={`Estimate your rollup ratio: ${datasource}`}
      >
        {loading ? (
          <>
            <div className="loader">
              <Loader loading />
            </div>
            <RollupRatioPanel
              datasource={datasource}
              queryColumns={queryColumns ? queryColumns : []}
              rollupRatio={-1}
              interval={interval}
              updateInterval={(interval: string) => this.updateInterval(interval)}
            />
          </>
        ) : (
          <>
            <div className="main-table">
              <ReactTable
                data={result ? result.queryResult.rows : []}
                loading={loading}
                columns={(result ? result.queryResult.header : []).map((h: any, i) => {
                  return {
                    Header: () => {
                      return (
                        <div
                          onClick={() => {
                            this.updateColumns(h);
                          }}
                        >
                          {h}
                        </div>
                      );
                    },
                    headerClassName: queryColumns.includes(h) ? h : 'selected',
                    accessor: String(i),
                    className: queryColumns.includes(h) ? h : 'selected',
                    Cell: row => <div> {row.value}</div>,
                  };
                })}
                showPageSizeOptions={false}
                showPagination={false}
                sortable={false}
              />
            </div>
            <RollupRatioPanel
              datasource={datasource}
              queryColumns={queryColumns}
              rollupRatio={rollupRatio}
              interval={interval}
              updateInterval={(interval: string) => this.updateInterval(interval)}
            />
          </>
        )}
      </Dialog>
    );
  }
}
