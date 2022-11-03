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
  FormGroup,
  HTMLSelect,
  IResizeEntry,
  Radio,
  RadioGroup,
  ResizeSensor,
} from '@blueprintjs/core';
import { AxisScale } from 'd3-axis';
import { scaleLinear, scaleUtc } from 'd3-scale';
import React from 'react';

import { Api } from '../../singletons';
import {
  Capabilities,
  ceilToUtcDay,
  formatBytes,
  formatInteger,
  queryDruidSql,
  QueryManager,
  uniq,
} from '../../utils';
import { DateRangeSelector } from '../date-range-selector/date-range-selector';
import { Loader } from '../loader/loader';

import { BarUnitData, StackedBarChart } from './stacked-bar-chart';

import './segment-timeline.scss';

interface SegmentTimelineProps {
  capabilities: Capabilities;

  // For testing:
  dataQueryManager?: QueryManager<
    { capabilities: Capabilities; startDate: Date; endDate: Date },
    any
  >;
}

type ActiveDataType = 'sizeData' | 'countData';

interface SegmentTimelineState {
  chartHeight: number;
  chartWidth: number;
  data?: Record<string, any>;
  datasources: string[];
  stackedData?: Record<string, BarUnitData[]>;
  singleDatasourceData?: Record<string, Record<string, BarUnitData[]>>;
  activeDatasource: string | null;
  activeDataType: ActiveDataType;
  dataToRender: BarUnitData[];
  loading: boolean;
  error?: Error;
  xScale: AxisScale<Date> | null;
  yScale: AxisScale<number> | null;
  startDate: Date;
  endDate: Date;
}

interface BarChartScales {
  xScale: AxisScale<Date>;
  yScale: AxisScale<number>;
}

interface IntervalRow {
  start: string;
  end: string;
  datasource: string;
  count: number;
  size: number;
}

const DEFAULT_TIME_SPAN_MONTHS = 3;

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

  static getSqlQuery(startDate: Date, endDate: Date): string {
    return `SELECT
  "start", "end", "datasource",
  COUNT(*) AS "count",
  SUM("size") AS "size"
FROM sys.segments
WHERE
  '${startDate.toISOString()}' <= "start" AND
  "end" <= '${endDate.toISOString()}' AND
  is_published = 1 AND
  is_overshadowed = 0
GROUP BY 1, 2, 3
ORDER BY "start" DESC`;
  }

  static processRawData(data: IntervalRow[]) {
    if (data === null) return [];

    const countData: Record<string, any> = {};
    const sizeData: Record<string, any> = {};
    data.forEach(entry => {
      const start = entry.start;
      const day = start.split('T')[0];
      const datasource = entry.datasource;
      const count = entry.count;
      const segmentSize = entry.size;
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
        const countDataEntry: number | undefined = countData[day][datasource];
        countData[day][datasource] = count + (countDataEntry === undefined ? 0 : countDataEntry);
        const sizeDataEntry: number | undefined = sizeData[day][datasource];
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

  static calculateStackedData(
    data: Record<string, any>,
    datasources: string[],
  ): Record<string, BarUnitData[]> {
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
            dailySize: d.total,
          };
          y0 += d[datasource] === undefined ? 0 : d[datasource];
          return barUnitData;
        });
      });
      newStackedData[type] = stackedData.flat();
    });

    return newStackedData;
  }

  static calculateSingleDatasourceData(
    data: Record<string, any>,
    datasources: string[],
  ): Record<string, Record<string, BarUnitData[]>> {
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
            dailySize: d.total,
          };
        });
        if (!dataResult.every((d: any) => d.y === 0)) {
          singleDatasourceData[dataType][datasource] = dataResult;
        }
      });
    });

    return singleDatasourceData;
  }

  private readonly dataQueryManager: QueryManager<
    { capabilities: Capabilities; startDate: Date; endDate: Date },
    any
  >;

  private readonly chartMargin = { top: 40, right: 15, bottom: 20, left: 60 };

  constructor(props: SegmentTimelineProps) {
    super(props);
    const startDate = ceilToUtcDay(new Date());
    const endDate = new Date(startDate.valueOf());
    startDate.setUTCMonth(startDate.getUTCMonth() - DEFAULT_TIME_SPAN_MONTHS);

    this.state = {
      chartWidth: 1, // Dummy init values to be replaced
      chartHeight: 1, // after first render
      data: {},
      datasources: [],
      stackedData: {},
      singleDatasourceData: {},
      dataToRender: [],
      activeDatasource: null,
      activeDataType: 'sizeData',
      loading: true,
      xScale: null,
      yScale: null,
      startDate,
      endDate,
    };

    this.dataQueryManager =
      props.dataQueryManager ||
      new QueryManager({
        processQuery: async ({ capabilities, startDate, endDate }) => {
          let intervals: IntervalRow[];
          let datasources: string[];
          if (capabilities.hasSql()) {
            intervals = await queryDruidSql({
              query: SegmentTimeline.getSqlQuery(startDate, endDate),
            });
            datasources = uniq(intervals.map(r => r.datasource));
          } else if (capabilities.hasCoordinatorAccess()) {
            const startIso = startDate.toISOString();

            datasources = (await Api.instance.get(`/druid/coordinator/v1/datasources`)).data;
            intervals = (
              await Promise.all(
                datasources.map(async datasource => {
                  const intervalMap = (
                    await Api.instance.get(
                      `/druid/coordinator/v1/datasources/${Api.encodePath(
                        datasource,
                      )}/intervals?simple`,
                    )
                  ).data;

                  return Object.keys(intervalMap)
                    .map(interval => {
                      const [start, end] = interval.split('/');
                      const { count, size } = intervalMap[interval];
                      return {
                        start,
                        end,
                        datasource,
                        count,
                        size,
                      };
                    })
                    .filter(a => startIso < a.start);
                }),
              )
            )
              .flat()
              .sort((a, b) => b.start.localeCompare(a.start));
          } else {
            throw new Error(`must have SQL or coordinator access`);
          }

          const data = SegmentTimeline.processRawData(intervals);
          const stackedData = SegmentTimeline.calculateStackedData(data, datasources);
          const singleDatasourceData = SegmentTimeline.calculateSingleDatasourceData(
            data,
            datasources,
          );
          return { data, datasources, stackedData, singleDatasourceData };
        },
        onStateChange: ({ data, loading, error }) => {
          this.setState({
            data: data ? data.data : undefined,
            datasources: data ? data.datasources : [],
            stackedData: data ? data.stackedData : undefined,
            singleDatasourceData: data ? data.singleDatasourceData : undefined,
            loading,
            error,
          });
        },
      });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;
    const { startDate, endDate } = this.state;

    this.dataQueryManager.runQuery({ capabilities, startDate, endDate });
  }

  componentWillUnmount(): void {
    this.dataQueryManager.terminate();
  }

  componentDidUpdate(_prevProps: SegmentTimelineProps, prevState: SegmentTimelineState): void {
    const { activeDatasource, activeDataType, singleDatasourceData, stackedData } = this.state;
    if (
      prevState.data !== this.state.data ||
      prevState.activeDataType !== this.state.activeDataType ||
      prevState.activeDatasource !== this.state.activeDatasource ||
      prevState.chartWidth !== this.state.chartWidth ||
      prevState.chartHeight !== this.state.chartHeight
    ) {
      const scales: BarChartScales | undefined = this.calculateScales();
      const dataToRender: BarUnitData[] | undefined = activeDatasource
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

  private calculateScales(): BarChartScales | undefined {
    const {
      chartWidth,
      chartHeight,
      data,
      activeDataType,
      activeDatasource,
      singleDatasourceData,
      startDate,
      endDate,
    } = this.state;
    if (!data || !Object.keys(data).length) return;
    const activeData = data[activeDataType];

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

    const xScale: AxisScale<Date> = scaleUtc()
      .domain([startDate, endDate])
      .range([0, chartWidth - this.chartMargin.left - this.chartMargin.right]);

    const yScale: AxisScale<number> = scaleLinear()
      .rangeRound([chartHeight - this.chartMargin.top - this.chartMargin.bottom, 0])
      .domain(yDomain);

    return {
      xScale,
      yScale,
    };
  }

  private readonly formatTick = (n: number) => {
    if (isNaN(n)) return '';
    const { activeDataType } = this.state;
    if (activeDataType === 'countData') {
      return formatInteger(n);
    } else {
      return formatBytes(n);
    }
  };

  private readonly handleResize = (entries: IResizeEntry[]) => {
    const chartRect = entries[0].contentRect;
    this.setState({
      chartWidth: chartRect.width,
      chartHeight: chartRect.height,
    });
  };

  renderStackedBarChart() {
    const {
      chartWidth,
      chartHeight,
      loading,
      dataToRender,
      activeDataType,
      error,
      xScale,
      yScale,
      data,
      activeDatasource,
      startDate,
      endDate,
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
          <span className="no-data-text">Error when loading data: {error.message}</span>
        </div>
      );
    }

    if (xScale === null || yScale === null) {
      return (
        <div>
          <span className="no-data-text">Error when calculating scales</span>
        </div>
      );
    }

    if (data![activeDataType].length === 0) {
      return (
        <div>
          <span className="no-data-text">No data available for the time span selected</span>
        </div>
      );
    }

    if (
      activeDatasource !== null &&
      data![activeDataType].every((d: any) => d[activeDatasource] === undefined)
    ) {
      return (
        <div>
          <span className="no-data-text">
            No data available for <i>{activeDatasource}</i>
          </span>
        </div>
      );
    }

    const millisecondsPerDay = 24 * 60 * 60 * 1000;
    const barCounts = (endDate.getTime() - startDate.getTime()) / millisecondsPerDay;
    const barWidth = Math.max(
      0,
      (chartWidth - this.chartMargin.left - this.chartMargin.right) / barCounts,
    );
    return (
      <ResizeSensor onResize={this.handleResize}>
        <StackedBarChart
          dataToRender={dataToRender}
          svgHeight={chartHeight}
          svgWidth={chartWidth}
          margin={this.chartMargin}
          changeActiveDatasource={(datasource: string | null) =>
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
      </ResizeSensor>
    );
  }

  render(): JSX.Element {
    const { capabilities } = this.props;
    const { datasources, activeDataType, activeDatasource, startDate, endDate } = this.state;

    return (
      <div className="segment-timeline app-view">
        {this.renderStackedBarChart()}
        <div className="side-control">
          <FormGroup>
            <RadioGroup
              onChange={(e: any) => this.setState({ activeDataType: e.target.value })}
              selectedValue={activeDataType}
            >
              <Radio label="Total size" value="sizeData" />
              <Radio label="Segment count" value="countData" />
            </RadioGroup>
          </FormGroup>

          <FormGroup label="Datasource">
            <HTMLSelect
              onChange={(e: any) =>
                this.setState({
                  activeDatasource: e.target.value === 'all' ? null : e.target.value,
                })
              }
              value={activeDatasource == null ? 'all' : activeDatasource}
              fill
            >
              <option value="all">Show all</option>
              {datasources.map(d => {
                return (
                  <option key={d} value={d}>
                    {d}
                  </option>
                );
              })}
            </HTMLSelect>
          </FormGroup>

          <FormGroup label="Interval">
            <DateRangeSelector
              startDate={startDate}
              endDate={endDate}
              onChange={(startDate, endDate) => {
                this.setState({ startDate, endDate }, () => {
                  this.dataQueryManager.runQuery({ capabilities, startDate, endDate });
                });
              }}
            />
          </FormGroup>
        </div>
      </div>
    );
  }
}
