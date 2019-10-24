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
  Button,
  Callout,
  FormGroup,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Position,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import * as React from 'react';

import { getDruidErrorMessage, queryDruidRune, QueryManager } from '../../utils/index';

import './rollup-ratio-panel.scss';

export interface RollupRatioPanelProps {
  queryColumns: string[];
  rollupRatio: number;
  datasource: string;
  interval: string;
  updateInterval: (interval: string) => void;
}

export interface RollupRatioPanelState {
  result?: number;
  intervalInput: string;
  granularity: string;
}

export class RollupRatioPanel extends React.PureComponent<
  RollupRatioPanelProps,
  RollupRatioPanelState
> {
  private druidQueryManager: QueryManager<null, number>;
  constructor(props: RollupRatioPanelProps, context: any) {
    super(props, context);
    this.state = {
      intervalInput: this.props.interval,
      granularity: 'NONE',
    };
    this.druidQueryManager = new QueryManager({
      processQuery: async (): Promise<number> => {
        const { granularity } = this.state;
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
              { type: 'count', name: 'Count' },
              {
                type: 'cardinality',
                name: 'DistCount',
                fields: queryColumns.map(field => {
                  if (field !== '__time') return field;
                  return {
                    type: 'extraction',
                    dimension: '__time',
                    extractionFn: {
                      type: 'timeFormat',
                      granularity: granularity,
                    },
                  };
                }),
                byRow: true,
              },
            ],
            postAggregations: [
              {
                type: 'expression',
                name: 'Ratio',
                expression: '(("Count" / "DistCount") * 1.0)',
                ordering: null,
              },
            ],
          });

          if (Array.isArray(timeseriesResponse) && timeseriesResponse.length === 1) {
            rawQueryResult = timeseriesResponse[0];
          } else {
            throw new Error(`unexpected response from segmentMetadata query`);
          }
        } catch (e) {
          throw new Error(getDruidErrorMessage(e));
        }

        return rawQueryResult.result.Ratio;
      },
      onStateChange: ({ result }) => {
        this.setState({
          result,
        });
      },
    });
  }
  componentDidMount() {
    const { queryColumns } = this.props;
    if (queryColumns.length !== 0) this.druidQueryManager.runQuery(null);
  }

  componentWillUnmount() {
    this.druidQueryManager.terminate();
  }
  componentDidUpdate(prevProps: RollupRatioPanelProps) {
    const { queryColumns } = this.props;
    if (prevProps.queryColumns.length === 0 && prevProps.queryColumns !== queryColumns) {
      this.druidQueryManager.runQuery(null);
    }
  }

  render(): JSX.Element {
    const { granularity, intervalInput, result } = this.state;
    const { rollupRatio, updateInterval } = this.props;
    return (
      <>
        <div className="rollup-ratio-panel">
          <Callout>
            <p>
              {`You may select any column to exclude them from your rollup preview. This will update
              your rollup ratio after you click on "Estimate rollup".`}
            </p>
            <p>{`Please click on "Preview data" after modifying your interval.`}</p>
            <p>
              {rollupRatio !== -1
                ? `This datasource has previously been rolled up. The original rollup ratio is ${(
                    rollupRatio * 100
                  ).toFixed(2)}%`
                : ''}
            </p>
          </Callout>
          <FormGroup label={`Interval`}>
            <InputGroup
              value={intervalInput}
              placeholder="2019-01-01/2020-01-01"
              onChange={(e: any) => {
                this.setState({ intervalInput: e.target.value });
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
          <Callout>
            <p>
              {`Estimated rollup ratio: `}
              {result ? (Math.max(result, 1) * 100).toFixed(2) + '%' : ''}
            </p>
          </Callout>
          <FormGroup label={`Granularity:`}>
            <InputGroup
              value={granularity}
              readOnly
              rightElement={
                <Popover
                  content={
                    <Menu>
                      <MenuItem
                        text="NONE"
                        onClick={() => this.setState({ granularity: 'NONE' })}
                      />
                      <MenuItem
                        text="MINUTE"
                        onClick={() => this.setState({ granularity: 'MINUTE' })}
                      />
                      <MenuItem
                        text="HOUR"
                        onClick={() => this.setState({ granularity: 'HOUR' })}
                      />
                      <MenuItem text="DAY" onClick={() => this.setState({ granularity: 'DAY' })} />
                    </Menu>
                  }
                  position={Position.BOTTOM_RIGHT}
                  autoFocus={false}
                >
                  <Button icon={IconNames.CARET_DOWN} minimal />
                </Popover>
              }
            />
          </FormGroup>
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
