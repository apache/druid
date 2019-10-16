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

import { RollupRatio } from './rollup-ratio';

import './rollup-estimate-dialog.scss';

export interface RollupEstimateDialogProps {
  datasource: string;
  onClose: () => void;
}

export interface RollupEstimateDialogState {
  queryColumns: string[];
  loading: boolean;
  result?: HeaderRows;
  error?: string;
}

export class RollupEstimateDialog extends React.PureComponent<
  RollupEstimateDialogProps,
  RollupEstimateDialogState
> {
  private druidQueryManager: QueryManager<null, HeaderRows>;

  constructor(props: RollupEstimateDialogProps, context: any) {
    super(props, context);
    this.state = {
      // Rename some of these variables
      //       queryString: `
      // {
      //   "queryType":"segmentMetadata",
      //   "dataSource":"${props.datasource}",
      //   "intervals":["2013-01-01/2020-01-01"],
      //   "analysisTypes": ["queryGranularity", "aggregators", "rollup"],
      //   "lenientAggregatorMerge": true,
      //   "merge": true
      // }
      //       `,

      queryColumns: [],
      loading: false,
    };

    this.druidQueryManager = new QueryManager({
      processQuery: async (): Promise<HeaderRows> => {
        const { datasource } = this.props;
        let rawQueryResult: any;
        try {
          const segmentMetadataResponse = await queryDruidRune({
            queryType: 'segmentMetadata',
            dataSource: datasource,
            intervals: ['2013-01-01/2020-01-01'],
            merge: true,
            lenientAggregatorMerge: true,
            analysisTypes: ['timestampSpec', 'queryGranularity', 'aggregators', 'rollup'],
          });

          if (Array.isArray(segmentMetadataResponse) && segmentMetadataResponse.length === 1) {
            const segmentMetadataResponse0 = segmentMetadataResponse[0];
            // console.log(segmentMetadataResponse0.aggregators);
            // console.log(segmentMetadataResponse0.columns);
            // console.log(
            //   Object.keys(segmentMetadataResponse0.columns).filter((column: any) =>
            //     Object.keys(segmentMetadataResponse0.aggregators).includes(column),
            //   ),
            // );
            try {
              const previewDataResponse = await queryDruidRune({
                queryType: 'scan',
                dataSource: datasource,
                intervals: ['2013-01-01/2020-01-01'],
                resultFormat: 'compactedList',
                limit: 20,
                columns: Object.keys(segmentMetadataResponse0.columns).filter(
                  (column: any) =>
                    !Object.keys(segmentMetadataResponse0.aggregators).includes(column),
                ),
              });
              console.log(previewDataResponse);
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

        // console.log(rawQueryResult);
        return queryResult;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          result,
          loading,
          error,
          queryColumns: result ? result.header : [],
        });
      },
    });
  }

  updateColumns(columnName: string) {
    const { queryColumns } = this.state;
    const newQueryColumns = queryColumns.slice();
    if (!queryColumns.includes(columnName)) {
      newQueryColumns.push(columnName);
    } else {
      const indexOfColumn = newQueryColumns.indexOf(columnName);
      newQueryColumns.splice(indexOfColumn, 1);
    }
    this.setState({ queryColumns: newQueryColumns });
  }

  componentDidMount() {
    // Do something when component unmounts
    this.druidQueryManager.runQuery(null);
  }
  render(): JSX.Element {
    // Plan to separate main and control components
    const { datasource, onClose } = this.props;
    const { result, loading, queryColumns } = this.state;
    if (loading) return <Loader />;
    // console.log(result);
    return (
      <Dialog
        className="rollup-estimate-dialog"
        isOpen={datasource !== undefined}
        onClose={onClose}
        canOutsideClickClose={false}
        title={`Estimate your rollup ratio: ${datasource}`}
      >
        <ReactTable
          data={result ? result.rows : []}
          loading={loading}
          columns={(result ? result.header : []).map((h: any, i) => {
            // Need to clean this up

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
        <RollupRatio datasource={datasource} queryColumns={queryColumns ? queryColumns : []} />
      </Dialog>
    );
  }
}
/*
Todo:
2. Correct query to calculate rollup ratio based on rollup/non-rollup data
3. Bucket time column for non-rolled data
4. Clean up code
5. Separate ratio from Callout, put name of datasource at top
6. Error handling with division of 0

*/
