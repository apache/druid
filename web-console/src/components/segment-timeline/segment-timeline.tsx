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

import { FormGroup, HTMLSelect, Radio, RadioGroup } from '@blueprintjs/core';
import * as d3 from 'd3';
import { AxisScale } from 'd3';
import React from 'react';

import { formatBytes, queryDruidSql, QueryManager } from '../../utils/index';
import { StackedBarChart } from '../../visualization/stacked-bar-chart';
import { Loader } from '../loader/loader';

import './segment-timeline.scss';

interface SegmentTimelineProps {
  chartHeight: number;
  chartWidth: number;
}

interface SegmentTimelineState {
  data?: Record<string, any>;
  datasources: string[];
  stackedData?: Record<string, BarUnitData[]>;
  singleDatasourceData?: Record<string, Record<string, BarUnitData[]>>;
  activeDatasource: string | null;
  activeDataType: string; // "countData" || "sizeData"
  dataToRender: BarUnitData[];
  timeSpan: number; // by months
  loading: boolean;
  error?: string;
  xScale: AxisScale<Date> | null;
  yScale: AxisScale<number> | null;
  dStart: Date;
  dEnd: Date;
}

interface BarChartScales {
  xScale: AxisScale<Date>;
  yScale: AxisScale<number>;
}

export interface BarUnitData {
  x: number;
  y: number;
  y0?: number;
  width: number;
  datasource: string;
  color: string;
}

export interface BarChartMargin {
  top: number;
  right: number;
  bottom: number;
  left: number;
}

export class SegmentTimeline extends React.PureComponent<
  SegmentTimelineProps,
  SegmentTimelineState
> {
  static COLORS = [
    '#b33040',
    '#d25c4d',
    '#f2b447',
    '#d9d574',
    '#4FAA7E',
    '#57ceff',
    '#789113',
    '#098777',
    '#b33040',
    '#d2757b',
    '#f29063',
    '#d9a241',
    '#80aa61',
    '#c4ff9e',
    '#915412',
    '#87606c',
  ];

  static getColor(index: number): string {
    return SegmentTimeline.COLORS[index % SegmentTimeline.COLORS.length];
  }

  private dataQueryManager: QueryManager<null, any>;
  private datasourceQueryManager: QueryManager<null, any>;
  private chartMargin = { top: 20, right: 10, bottom: 20, left: 10 };

  constructor(props: SegmentTimelineProps) {
    super(props);
    const dStart = new Date();
    const dEnd = new Date();
    dStart.setMonth(dStart.getMonth() - 3);
    this.state = {
      data: {},
      datasources: [],
      stackedData: {},
      singleDatasourceData: {},
      dataToRender: [],
      activeDatasource: null,
      activeDataType: 'countData',
      timeSpan: 3,
      loading: true,
      xScale: null,
      yScale: null,
      dEnd: dEnd,
      dStart: dStart,
    };

    this.dataQueryManager = new QueryManager({
      processQuery: async () => {
        const { timeSpan } = this.state;
        const query = `SELECT "start", "end", "datasource", COUNT(*) AS "count", sum(size) as "total_size"
              FROM sys.segments
              WHERE "start" > time_format(TIMESTAMPADD(MONTH, -${timeSpan}, current_timestamp), 'yyyy-MM-dd''T''hh:mm:ss.SSS')
              GROUP BY 1, 2, 3
              ORDER BY "start" DESC`;
        const resp: any[] = await queryDruidSql({ query });
        const data = this.processRawData(resp);
        const stackedData = this.calculateStackedData(data);
        const singleDatasourceData = this.calculateSingleDatasourceData(data);
        return { data, stackedData, singleDatasourceData };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          data: result ? result.data : undefined,
          stackedData: result ? result.stackedData : undefined,
          singleDatasourceData: result ? result.singleDatasourceData : undefined,
          loading,
          error,
        });
      },
    });

    this.datasourceQueryManager = new QueryManager({
      processQuery: async () => {
        const query = `SELECT DISTINCT "datasource" FROM sys.segments`;
        const resp: any[] = await queryDruidSql({ query });
        const data = resp.map((r: any) => r.datasource);
        return data;
      },
      onStateChange: ({ result }) => {
        if (result == null) result = [];
        this.setState({
          datasources: result,
        });
      },
    });
  }

  componentDidMount(): void {
    this.dataQueryManager.runQuery(null);
    this.datasourceQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.dataQueryManager.terminate();
    this.datasourceQueryManager.terminate();
  }

  componentDidUpdate(prevProps: SegmentTimelineProps, prevState: SegmentTimelineState): void {
    const { activeDatasource, activeDataType, singleDatasourceData, stackedData } = this.state;
    if (
      prevState.data !== this.state.data ||
      prevState.activeDataType !== this.state.activeDataType ||
      prevState.activeDatasource !== this.state.activeDatasource ||
      prevProps.chartWidth !== this.props.chartWidth ||
      prevProps.chartHeight !== this.props.chartHeight
    ) {
      const scales: BarChartScales | undefined = this.calculateScales();
      let dataToRender: BarUnitData[] | undefined;
      dataToRender = activeDatasource
        ? singleDatasourceData
          ? singleDatasourceData[activeDataType][activeDatasource]
          : undefined
        : stackedData
        ? stackedData[activeDataType]
        : undefined;

      if (scales && dataToRender) {
        this.setState({
          dataToRender,
          xScale: scales.xScale,
          yScale: scales.yScale,
        });
      }
    }
  }

  private processRawData(data: any) {
    if (data === null) return [];
    const countData: Record<string, any> = {};
    const sizeData: Record<string, any> = {};
    data.forEach((entry: any) => {
      const start = entry.start;
      const day = start.split('T')[0];
      const datasource = entry.datasource;
      const count = entry.count;
      const segmentSize = entry['total_size'];
      if (countData[day] === undefined) {
        countData[day] = {
          day,
          [datasource]: count,
          total: count,
        };
        sizeData[day] = {
          day,
          [datasource]: segmentSize,
          total: segmentSize,
        };
      } else {
        const countDataEntry = countData[day][datasource];
        countData[day][datasource] = count + (countDataEntry === undefined ? 0 : countDataEntry);
        const sizeDataEntry = sizeData[day][datasource];
        sizeData[day][datasource] = segmentSize + (sizeDataEntry === undefined ? 0 : sizeDataEntry);
        countData[day].total += count;
        sizeData[day].total += segmentSize;
      }
    });
    const countDataArray = Object.keys(countData)
      .reverse()
      .map((time: any) => {
        return countData[time];
      });
    const sizeDataArray = Object.keys(sizeData)
      .reverse()
      .map((time: any) => {
        return sizeData[time];
      });
    return { countData: countDataArray, sizeData: sizeDataArray };
  }

  private calculateStackedData(data: Record<string, any>): Record<string, BarUnitData[]> {
    const { datasources } = this.state;
    const newStackedData: Record<string, BarUnitData[]> = {};
    Object.keys(data).forEach((type: any) => {
      const stackedData: any = data[type].map((d: any) => {
        let y0 = 0;
        return datasources.map((datasource: string, i) => {
          const barUnitData = {
            x: d.day,
            y: d[datasource] === undefined ? 0 : d[datasource],
            y0,
            datasource,
            color: SegmentTimeline.getColor(i),
          };
          y0 += d[datasource] === undefined ? 0 : d[datasource];
          return barUnitData;
        });
      });
      newStackedData[type] = stackedData.flat();
    });
    return newStackedData;
  }

  private calculateSingleDatasourceData(
    data: Record<string, any>,
  ): Record<string, Record<string, BarUnitData[]>> {
    const { datasources } = this.state;
    const singleDatasourceData: Record<string, Record<string, BarUnitData[]>> = {};
    Object.keys(data).forEach(dataType => {
      singleDatasourceData[dataType] = {};
      datasources.forEach((datasource, i) => {
        const currentData = data[dataType];
        if (currentData.length === 0) return;
        const dataResult = currentData.map((d: any) => {
          let y = 0;
          if (d[datasource] !== undefined) {
            y = d[datasource];
          }
          return {
            x: d.day,
            y,
            datasource,
            color: SegmentTimeline.getColor(i),
          };
        });
        if (!dataResult.every((d: any) => d.y === 0)) {
          singleDatasourceData[dataType][datasource] = dataResult;
        }
      });
    });
    return singleDatasourceData;
  }

  private calculateScales(): BarChartScales | undefined {
    const { chartWidth, chartHeight } = this.props;
    const {
      data,
      activeDataType,
      activeDatasource,
      singleDatasourceData,
      dStart,
      dEnd,
    } = this.state;
    if (!data || !Object.keys(data).length) return;
    const activeData = data[activeDataType];
    const xDomain: Date[] = [dStart, dEnd];
    let yDomain: number[] = [
      0,
      activeData.length === 0
        ? 0
        : activeData.reduce((max: any, d: any) => (max.total > d.total ? max : d)).total,
    ];

    if (
      activeDatasource !== null &&
      singleDatasourceData![activeDataType][activeDatasource] !== undefined
    ) {
      yDomain = [
        0,
        singleDatasourceData![activeDataType][activeDatasource].reduce((max: any, d: any) =>
          max.y > d.y ? max : d,
        ).y,
      ];
    }

    const xScale: AxisScale<Date> = d3
      .scaleTime()
      .domain(xDomain)
      .range([0, chartWidth - this.chartMargin.left - this.chartMargin.right]);

    const yScale: AxisScale<number> = d3
      .scaleLinear()
      .rangeRound([chartHeight - this.chartMargin.top - this.chartMargin.bottom, 0])
      .domain(yDomain);

    return {
      xScale,
      yScale,
    };
  }

  onTimeSpanChange = (e: any) => {
    const dStart = new Date();
    const dEnd = new Date();
    dStart.setMonth(dStart.getMonth() - e);
    this.setState({
      timeSpan: e,
      loading: true,
      dStart,
      dEnd,
    });
    this.dataQueryManager.rerunLastQuery();
  };

  formatTick = (n: number) => {
    const { activeDataType } = this.state;
    if (activeDataType === 'countData') {
      return n.toString();
    } else {
      return formatBytes(n);
    }
  };

  renderStackedBarChart() {
    const { chartWidth, chartHeight } = this.props;
    const {
      loading,
      dataToRender,
      activeDataType,
      error,
      xScale,
      yScale,
      data,
      activeDatasource,
      dStart,
      dEnd,
    } = this.state;
    if (loading) {
      return (
        <div>
          <Loader loading={loading} />
        </div>
      );
    }

    if (error) {
      return (
        <div>
          <span className={'no-data-text'}>Error when loading data: {error}</span>
        </div>
      );
    }

    if (xScale === null || yScale === null) {
      return (
        <div>
          <span className={'no-data-text'}>Error when calculating scales</span>
        </div>
      );
    }

    if (data![activeDataType].length === 0) {
      return (
        <div>
          <span className={'no-data-text'}>No data available for the time span selected</span>
        </div>
      );
    }

    if (
      activeDatasource !== null &&
      data![activeDataType].every((d: any) => d[activeDatasource] === undefined)
    ) {
      return (
        <div>
          <span className={'no-data-text'}>
            No data available for <i>{activeDatasource}</i>
          </span>
        </div>
      );
    }

    const millisecondsPerDay = 24 * 60 * 60 * 1000;
    const barCounts = (dEnd.getTime() - dStart.getTime()) / millisecondsPerDay;
    const barWidth = (chartWidth - this.chartMargin.left - this.chartMargin.right) / barCounts;
    return (
      <StackedBarChart
        dataToRender={dataToRender}
        svgHeight={chartHeight}
        svgWidth={chartWidth}
        margin={this.chartMargin}
        changeActiveDatasource={(datasource: string) =>
          this.setState(prevState => ({
            activeDatasource: prevState.activeDatasource ? null : datasource,
          }))
        }
        activeDataType={activeDataType}
        formatTick={(n: number) => this.formatTick(n)}
        xScale={xScale}
        yScale={yScale}
        barWidth={barWidth}
      />
    );
  }

  render(): JSX.Element {
    const { datasources, activeDataType, activeDatasource, timeSpan } = this.state;

    return (
      <div className={'segment-timeline app-view'}>
        {this.renderStackedBarChart()}
        <div className={'side-control'}>
          <FormGroup>
            <RadioGroup
              onChange={(e: any) => this.setState({ activeDataType: e.target.value })}
              selectedValue={activeDataType}
            >
              <Radio label={'Segment count'} value={'countData'} />
              <Radio label={'Total size'} value={'sizeData'} />
            </RadioGroup>
          </FormGroup>

          <FormGroup label={'Datasource:'}>
            <HTMLSelect
              onChange={(e: any) =>
                this.setState({
                  activeDatasource: e.target.value === 'all' ? null : e.target.value,
                })
              }
              value={activeDatasource == null ? 'all' : activeDatasource}
              fill
            >
              <option value={'all'}>Show all</option>
              {datasources.map(d => {
                return (
                  <option key={d} value={d}>
                    {d}
                  </option>
                );
              })}
            </HTMLSelect>
          </FormGroup>

          <FormGroup label={'Period:'}>
            <HTMLSelect
              onChange={(e: any) => this.onTimeSpanChange(e.target.value)}
              value={timeSpan}
              fill
            >
              <option value={1}> 1 months</option>
              <option value={3}> 3 months</option>
              <option value={6}> 6 months</option>
              <option value={9}> 9 months</option>
              <option value={12}> 1 year</option>
            </HTMLSelect>
          </FormGroup>
        </div>
      </div>
    );
  }
}
