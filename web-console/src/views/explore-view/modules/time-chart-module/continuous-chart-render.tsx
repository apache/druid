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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Timezone } from 'chronoshift';
import { day, Duration, minute, second } from 'chronoshift';
import classNames from 'classnames';
import { max, sort, sum } from 'd3-array';
import { axisBottom } from 'd3-axis';
import { scaleLinear, scaleUtc } from 'd3-scale';
import { select } from 'd3-selection';
import type { MouseEvent as ReactMouseEvent } from 'react';
import { useMemo, useRef, useState } from 'react';

import type { PortalBubbleOpenOn } from '../../../../components';
import { PortalBubble } from '../../../../components';
import { useClock, useGlobalEventListener } from '../../../../hooks';
import type { Margin } from '../../../../utils';
import {
  clamp,
  formatIsoDateRange,
  formatNumber,
  formatStartDuration,
  lookupBy,
  minBy,
  Stage,
  tickFormatWithTimezone,
  timezoneAwareTicks,
} from '../../../../utils';
import type { ExpressionMeta } from '../../models';

import { ContinuousChartSingleRender } from './continuous-chart-single-render';

import './continuous-chart-render.scss';

const Y_AXIS_WIDTH = 60;
const MEASURE_GAP = 6;

function getDefaultChartMargin(yAxis: undefined | 'left' | 'right') {
  return {
    top: 10,
    right: 10 + (yAxis === 'right' ? Y_AXIS_WIDTH : 0),
    bottom: 25,
    left: 10 + (yAxis === 'left' ? Y_AXIS_WIDTH : 0),
  };
}

const EXTEND_X_SCALE_DOMAIN_BY = 1;

// ---------------------------------------

export type Range = [number, number];

export interface RangeDatum {
  start: number;
  end: number;
  measures: number[];
  facet: string | undefined;
}

export interface StackedRangeDatum extends RangeDatum {
  offset: number;
}

// ---------------------------------------

const DAY_DURATION = new Duration('P1D');

function getTodayRange(timezone: Timezone): Range {
  const [start, end] = DAY_DURATION.range(new Date(), timezone);
  return [start.valueOf(), end.valueOf()];
}

function offsetRange(dateRange: Range, offset: number, roundEnd?: (n: number) => number): Range {
  const d = dateRange[1] - dateRange[0];
  let newEnd = dateRange[1] + offset;
  if (roundEnd) newEnd = roundEnd(newEnd);
  return [newEnd - d, newEnd];
}

interface SelectionRange {
  start: number;
  end: number;
  finalized?: boolean;
  selectedDatum?: StackedRangeDatum;
  measureIndex?: number;
}

export type ContinuousChartMarkType = 'bar' | 'area' | 'line';
export type ContinuousChartCurveType = 'smooth' | 'linear' | 'step';

export interface ContinuousChartRenderProps {
  /**
   * The data to be rendered it has to be ordered in reverse chronological order (latest first)
   * If stacking is used then the stack bars should be ordered bottom to top.
   */
  data: RangeDatum[];
  facets: string[] | undefined;
  facetColorizer: (facet: string) => string;

  /**
   * The granularity that was used for bucketing.
   */
  granularity: Duration;
  markType: ContinuousChartMarkType;

  /**
   * Defines how to render the curve in case 'area' or 'line' is selected as the mark type
   */
  curveType?: ContinuousChartCurveType;

  /**
   * The measures being displayed
   */
  measures: readonly ExpressionMeta[];

  /**
   * The width x height to render
   */
  stage: Stage;
  margin?: Margin;
  timezone: Timezone;

  yAxisPosition?: 'left' | 'right';
  showHorizontalGridlines?: 'auto' | 'always' | 'never';

  /**
   * The optional range of the x-axis to show, if not set it defaults to the extent of the data
   */
  domainRange: Range | undefined;
  onChangeRange(range: Range): void;
}

export const ContinuousChartRender = function ContinuousChartRender(
  props: ContinuousChartRenderProps,
) {
  const {
    data,
    facets,
    facetColorizer,
    granularity,

    markType,
    curveType,
    measures,

    stage,
    margin,
    timezone,
    yAxisPosition,
    showHorizontalGridlines,
    domainRange,
    onChangeRange,
  } = props;

  const numMeasures = measures.length;
  const [mouseDownAt, setMouseDownAt] = useState<
    { time: number; action: 'select' | 'shift' } | undefined
  >();
  const [selection, setSelection] = useState<SelectionRange | undefined>();

  function setSelectionIfNeeded(newSelection: SelectionRange) {
    if (
      selection &&
      selection.start === newSelection.start &&
      selection.end === newSelection.end &&
      selection.finalized === newSelection.finalized &&
      selection.selectedDatum === newSelection.selectedDatum
    ) {
      return;
    }
    setSelection(newSelection);
  }

  const [shiftOffset, setShiftOffset] = useState<number | undefined>();

  const now = useClock(minute.canonicalLength);
  const svgRef = useRef<SVGSVGElement | null>(null);

  const stackedDataByMeasure: StackedRangeDatum[][] = useMemo(() => {
    const effectiveFacet = facets || ['undefined'];
    const facetToIndex = lookupBy(
      effectiveFacet,
      f => f,
      (_, i) => i,
    );

    // Sort the data into time descending column and stack order
    const sortedData = sort(data, (a, b) => {
      const diffStart = b.start - a.start;
      if (diffStart) return diffStart;

      return facetToIndex[String(a.facet)] - facetToIndex[String(b.facet)];
    });

    // Create stacked data for each measure
    return Array.from({ length: numMeasures }, (_, measureIndex) => {
      if (markType === 'line') {
        // No need to stack
        return sortedData.map(d => ({ ...d, offset: 0 }));
      } else {
        let lastStart: number | undefined;
        let offset: number;
        return sortedData.map(d => {
          if (lastStart !== d.start) {
            offset = 0;
            lastStart = d.start;
          }
          const withOffset = { ...d, offset };
          offset += d.measures[measureIndex];
          return withOffset;
        });
      }
    });
  }, [data, facets, markType, numMeasures]);

  function findStackedDatum(
    time: number,
    measure: number,
    measureIndex: number,
    isStacked: boolean,
  ): StackedRangeDatum | undefined {
    const stackedData = stackedDataByMeasure[measureIndex];
    if (!stackedData) return undefined;

    const dataInRange = stackedData.filter(d => d.start <= time && time < d.end);
    if (!dataInRange.length) return;
    if (isStacked) {
      return (
        dataInRange.find(
          r => r.offset <= measure && measure < r.measures[measureIndex] + r.offset,
        ) || dataInRange[dataInRange.length - 1]
      );
    } else {
      return minBy(dataInRange, r => Math.abs(r.measures[measureIndex] - measure));
    }
  }

  const chartMargin = { ...margin, ...getDefaultChartMargin(yAxisPosition) };

  // Calculate height for each chart based on number of measures
  const totalGapHeight = (numMeasures - 1) * MEASURE_GAP;
  const chartHeight = Math.floor(
    (stage.height - chartMargin.top - chartMargin.bottom - totalGapHeight) / numMeasures,
  );
  const singleChartStage = new Stage(stage.width, chartHeight);
  const innerStage = singleChartStage.applyMargin({
    top: 0,
    right: chartMargin.right,
    bottom: 0,
    left: chartMargin.left,
  });

  const allStackedData = stackedDataByMeasure.flat();
  const effectiveDomainRange =
    domainRange ||
    (allStackedData.length
      ? [allStackedData[allStackedData.length - 1].start, allStackedData[0].end]
      : getTodayRange(timezone));

  const baseTimeScale = scaleUtc()
    .domain(effectiveDomainRange)
    .range([EXTEND_X_SCALE_DOMAIN_BY, innerStage.width - EXTEND_X_SCALE_DOMAIN_BY]);
  const timeScale = shiftOffset
    ? baseTimeScale.copy().domain(offsetRange(effectiveDomainRange, shiftOffset))
    : baseTimeScale;

  // Create a scale for each measure
  const measureScales = useMemo(
    () =>
      stackedDataByMeasure.map((stackedData, measureIndex) => {
        const maxMeasure = max(stackedData, d => d.measures[measureIndex] + d.offset);
        return scaleLinear()
          .rangeRound([innerStage.height, 0])
          .domain([0, (maxMeasure ?? 100) * 1.05]);
      }),
    [stackedDataByMeasure, innerStage.height],
  );

  function getMeasureIndexFromY(y: number): number {
    const totalChartsHeight = chartHeight * numMeasures + totalGapHeight;
    if (y < 0 || y > totalChartsHeight) return 0;
    const index = Math.floor(y / (chartHeight + MEASURE_GAP));
    return Math.min(index, numMeasures - 1);
  }

  function handleMouseDown(e: ReactMouseEvent) {
    const svg = svgRef.current;
    if (!svg) return;
    e.preventDefault();

    const rect = svg.getBoundingClientRect();
    const x = clamp(
      e.clientX - rect.x - chartMargin.left,
      EXTEND_X_SCALE_DOMAIN_BY,
      innerStage.width - EXTEND_X_SCALE_DOMAIN_BY,
    );
    const y = e.clientY - rect.y - chartMargin.top;
    const time = baseTimeScale.invert(x).valueOf();
    const totalChartsHeight = chartHeight * numMeasures + totalGapHeight;
    const action = y > totalChartsHeight || e.shiftKey ? 'shift' : 'select';
    setMouseDownAt({
      time,
      action,
    });
    if (action === 'select') {
      const start = granularity.floor(new Date(time), timezone);
      setSelectionIfNeeded({
        start: start.valueOf(),
        end: granularity.shift(start, timezone, 1).valueOf(),
        measureIndex: getMeasureIndexFromY(y),
      });
    } else {
      setSelection(undefined);
    }
  }

  useGlobalEventListener('mousemove', (e: MouseEvent) => {
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const x = e.clientX - rect.x - chartMargin.left;
    const y = e.clientY - rect.y - chartMargin.top;

    if (mouseDownAt) {
      e.preventDefault();

      if (mouseDownAt.action === 'shift' || e.shiftKey) {
        const b = baseTimeScale.invert(x).valueOf();
        setShiftOffset(mouseDownAt.time.valueOf() - b.valueOf());
      } else {
        const b = baseTimeScale
          .invert(clamp(x, EXTEND_X_SCALE_DOMAIN_BY, innerStage.width - EXTEND_X_SCALE_DOMAIN_BY))
          .valueOf();
        const measureIndex = getMeasureIndexFromY(y);
        if (mouseDownAt.time < b) {
          setSelectionIfNeeded({
            start: granularity.floor(new Date(mouseDownAt.time), timezone).valueOf(),
            end: granularity.ceil(new Date(b), timezone).valueOf(),
            measureIndex,
          });
        } else {
          setSelectionIfNeeded({
            start: granularity.floor(new Date(b), timezone).valueOf(),
            end: granularity.ceil(new Date(mouseDownAt.time), timezone).valueOf(),
            measureIndex,
          });
        }
      }
    } else if (!selection?.finalized) {
      const totalChartsHeight = chartHeight * numMeasures + totalGapHeight;
      if (
        0 <= x &&
        x <= innerStage.width &&
        0 <= y &&
        y <= totalChartsHeight &&
        svg.contains(e.target as any)
      ) {
        const time = baseTimeScale.invert(x).valueOf();
        const measureIndex = getMeasureIndexFromY(y);
        const measureScale = measureScales[measureIndex];

        if (!measureScale) {
          setSelection(undefined);
          return;
        }

        const yInChart = y - measureIndex * (chartHeight + MEASURE_GAP);
        const measure = measureScale.invert(yInChart);

        const start = granularity.floor(new Date(time), timezone);
        const end = granularity.shift(start, timezone, 1);

        setSelectionIfNeeded({
          start: start.valueOf(),
          end: end.valueOf(),
          selectedDatum: findStackedDatum(time, measure, measureIndex, markType !== 'line'),
          measureIndex,
        });
      } else {
        setSelection(undefined);
      }
    }
  });

  useGlobalEventListener('mouseup', (e: MouseEvent) => {
    if (!mouseDownAt) return;
    e.preventDefault();
    setMouseDownAt(undefined);

    if (!shiftOffset && !selection) return;

    setShiftOffset(undefined);
    if (mouseDownAt.action === 'shift' || e.shiftKey) {
      if (shiftOffset) {
        const domainRangeExtent = effectiveDomainRange[1] - effectiveDomainRange[0];
        const snapGranularity =
          domainRangeExtent > granularity.getCanonicalLength() * 5 &&
          domainRangeExtent > second.canonicalLength
            ? granularity
            : new Duration('PT1S');
        onChangeRange(
          offsetRange(effectiveDomainRange, shiftOffset, n =>
            snapGranularity.round(new Date(n), timezone).valueOf(),
          ),
        );
      }
    } else {
      if (selection) {
        setSelection({
          ...selection,
          finalized: true,
        });
      }
    }
  });

  useGlobalEventListener('keydown', (e: KeyboardEvent) => {
    if (e.key === 'Escape') {
      setMouseDownAt(undefined);
      setSelection(undefined);
    }
  });

  if (innerStage.isInvalid()) return;

  function startEndToXWidth({ start, end }: { start: number; end: number }) {
    const xStart = timeScale(start);
    const xEnd = timeScale(end);
    if (xEnd < 0 || innerStage.width < xStart) return;

    return {
      x: xStart,
      width: Math.max(xEnd - xStart - 1, 1),
    };
  }

  let hoveredOpenOn: PortalBubbleOpenOn | undefined;
  if (selection) {
    const { start, end, selectedDatum, measureIndex = 0 } = selection;

    let title: string;
    let info: string;
    if (selectedDatum) {
      title = formatStartDuration(new Date(selectedDatum.start), granularity);
      info = formatNumber(selectedDatum.measures[measureIndex]);
    } else {
      if (granularity.shift(new Date(start), timezone).valueOf() === end) {
        title = formatStartDuration(new Date(start), granularity);
      } else {
        title = formatIsoDateRange(new Date(start), new Date(end), timezone);
      }

      const selectedData = stackedDataByMeasure[measureIndex].filter(
        d => start <= d.start && d.start < end,
      );
      if (selectedData.length) {
        info = formatNumber(sum(selectedData, b => b.measures[measureIndex]));
      } else {
        info = 'No data';
      }
    }

    hoveredOpenOn = {
      x: chartMargin.left + timeScale((selection.start + selection.end) / 2),
      y: chartMargin.top + measureIndex * (chartHeight + MEASURE_GAP),
      title,
      text: (
        <>
          {selectedDatum?.facet && <div>{selectedDatum?.facet}</div>}
          <div>{info}</div>
          {selection.finalized && (
            <div className="button-bar">
              <Button
                icon={IconNames.ZOOM_IN}
                text="Zoom in"
                intent={Intent.PRIMARY}
                small
                onClick={() => {
                  if (!selection) return;
                  setSelection(undefined);
                  onChangeRange([selection.start, selection.end]);
                }}
              />
            </div>
          )}
        </>
      ),
    };
  }

  const gridlinesVisible =
    showHorizontalGridlines === 'always' ||
    (showHorizontalGridlines !== 'never' && innerStage.height > 75);

  const d = effectiveDomainRange[1] - effectiveDomainRange[0];
  const shiftStartBack = effectiveDomainRange[0] - d;
  const shiftEndForward = effectiveDomainRange[1] + d;
  const nowDayCeil = day.ceil(now, timezone);
  const zoomedOutRange: Range = [
    shiftStartBack,
    shiftEndForward < nowDayCeil.valueOf() ? shiftEndForward : nowDayCeil.valueOf(),
  ];

  const nowX = timeScale(now);
  const totalChartsHeight = chartHeight * numMeasures + totalGapHeight;
  const xAxisHeight = 25;

  return (
    <div className="continuous-chart-render">
      <svg
        className="main-chart"
        ref={svgRef}
        {...stage.toWidthHeight()}
        viewBox={stage.toViewBox()}
        preserveAspectRatio="xMinYMin meet"
        onMouseDown={handleMouseDown}
      >
        <g transform={`translate(${chartMargin.left},${chartMargin.top})`}>
          {/* Render each measure's chart */}
          {stackedDataByMeasure.map((stackedData, measureIndex) => (
            <ContinuousChartSingleRender
              key={measureIndex}
              data={stackedData}
              measureIndex={measureIndex}
              facets={facets}
              facetColorizer={facetColorizer}
              markType={markType}
              curveType={curveType}
              timeScale={timeScale}
              measureScale={measureScales[measureIndex]}
              innerStage={innerStage}
              yAxisPosition={yAxisPosition}
              showHorizontalGridlines={gridlinesVisible}
              selectedDatum={
                selection?.measureIndex === measureIndex ? selection.selectedDatum : undefined
              }
              selectionFinalized={selection?.finalized}
              transform={`translate(0,${measureIndex * (chartHeight + MEASURE_GAP)})`}
              title={numMeasures > 1 ? measures[measureIndex].name : undefined}
            />
          ))}

          {/* Selection overlay across all charts */}
          <g clipPath={`xywh(0px 0px ${innerStage.width}px ${totalChartsHeight}px) view-box`}>
            {selection && (
              <rect
                className={classNames('selection', { finalized: selection.finalized })}
                {...startEndToXWidth(selection)}
                y={0}
                height={totalChartsHeight}
              />
            )}
            {0 < nowX && nowX < innerStage.width && (
              <line className="now-line" x1={nowX} x2={nowX} y1={0} y2={totalChartsHeight + 8} />
            )}
            {!!shiftOffset && (
              <rect
                className="shifter"
                x={shiftOffset > 0 ? timeScale(effectiveDomainRange[1]) : 0}
                y={0}
                height={totalChartsHeight}
                width={
                  shiftOffset > 0
                    ? innerStage.width - timeScale(effectiveDomainRange[1])
                    : timeScale(effectiveDomainRange[0])
                }
              />
            )}
          </g>

          {/* X-axis at the bottom */}
          <g
            className="axis-x"
            transform={`translate(0,${totalChartsHeight + 1})`}
            ref={(node: any) =>
              select(node).call(
                axisBottom(timeScale)
                  .tickValues(
                    timezoneAwareTicks(
                      new Date(effectiveDomainRange[0]),
                      new Date(effectiveDomainRange[1]),
                      10,
                      timezone,
                    ),
                  )
                  .tickFormat(x => tickFormatWithTimezone(x as Date, timezone)),
              )
            }
          />
          <rect
            className={classNames('time-shift-indicator', {
              shifting: typeof shiftOffset === 'number',
            })}
            x={0}
            y={totalChartsHeight}
            width={innerStage.width}
            height={xAxisHeight}
          />
        </g>
      </svg>
      {!data.length && (
        <div className="empty-placeholder">
          <div className="no-data-text">There is no data in the selected range</div>
        </div>
      )}
      <Button
        className="zoom-out-button"
        icon={IconNames.ZOOM_OUT}
        data-tooltip="Zoom out"
        small
        minimal
        onClick={() => {
          onChangeRange(zoomedOutRange);
        }}
      />
      {svgRef.current && (
        <PortalBubble
          className="continuous-chart-bubble"
          openOn={hoveredOpenOn}
          offsetElement={svgRef.current}
          onClose={selection?.finalized ? () => setSelection(undefined) : undefined}
          mute
          direction="up"
        />
      )}
    </div>
  );
};
