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
import { day, Duration, minute, second, Timezone } from 'chronoshift';
import classNames from 'classnames';
import { max, sort, sum } from 'd3-array';
import { axisBottom, axisLeft, axisRight } from 'd3-axis';
import { scaleLinear, scaleOrdinal, scaleUtc } from 'd3-scale';
import { select } from 'd3-selection';
import type { Area, Line } from 'd3-shape';
import { area, curveLinear, curveMonotoneX, curveStep, line } from 'd3-shape';
import type { MouseEvent as ReactMouseEvent } from 'react';
import { useMemo, useRef, useState } from 'react';

import type { PortalBubbleOpenOn } from '../../../../components';
import { PortalBubble } from '../../../../components';
import { useClock, useGlobalEventListener } from '../../../../hooks';
import type { Margin, Stage } from '../../../../utils';
import {
  clamp,
  filterMap,
  formatIsoDateRange,
  formatNumber,
  formatStartDuration,
  groupBy,
  lookupBy,
} from '../../../../utils';

import './continuous-chart-render.scss';

const Y_AXIS_WIDTH = 60;

function getDefaultChartMargin(yAxis: undefined | 'left' | 'right') {
  return {
    top: 20,
    right: 10 + (yAxis === 'right' ? Y_AXIS_WIDTH : 0),
    bottom: 25,
    left: 10 + (yAxis === 'left' ? Y_AXIS_WIDTH : 0),
  };
}

const EXTEND_X_SCALE_DOMAIN_BY = 1;

export const OTHER_VALUE = 'Other';
const OTHER_COLOR = '#666666';
const COLORS = ['#1b9e77', '#d95f02', '#7570b3', '#e7298a', '#66a61e', '#e6ab02', '#a6761d'];

// ---------------------------------------

export type Range = [number, number];

export interface RangeDatum {
  start: number;
  end: number;
  measure: number;
  stack: string | undefined;
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
}

export type ContinuousChartMarkType = 'bar' | 'area' | 'line';
export type ContinuousChartCurveType = 'smooth' | 'linear' | 'step';

function getCurveFactory(curveType: ContinuousChartCurveType | undefined) {
  switch (curveType) {
    case 'linear':
      return curveLinear;

    case 'step':
      return curveStep;

    case 'smooth':
    default:
      return curveMonotoneX;
  }
}

export interface ContinuousChartRenderProps {
  /**
   * The data to be rendered it has to be ordered in reverse chronological order (latest first)
   * If stacking is used then the stack bars should be ordered bottom to top.
   */
  data: RangeDatum[];
  stacks: string[] | undefined;

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
   * The width x height to render
   */
  stage: Stage;
  margin?: Margin;

  yAxis?: 'left' | 'right';
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
    stacks,
    granularity,

    markType,
    curveType,

    stage,
    margin,
    yAxis,
    showHorizontalGridlines,
    domainRange,
    onChangeRange,
  } = props;
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

  const stackedData: StackedRangeDatum[] = useMemo(() => {
    const effectiveStacks = stacks || ['undefined'];
    const stackToIndex = lookupBy(
      effectiveStacks,
      s => s,
      (_, i) => i,
    );

    // Sort the data into time descending column and stack order
    const sortedData = sort(data, (a, b) => {
      const diffStart = b.start - a.start;
      if (diffStart) return diffStart;

      return stackToIndex[String(a.stack)] - stackToIndex[String(b.stack)];
    });

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
        offset += d.measure;
        return withOffset;
      });
    }
  }, [data, stacks, markType]);

  function findStackedDatum(time: number, measure: number): StackedRangeDatum | undefined {
    const dataInRange = stackedData.filter(d => d.start <= time && time < d.end);
    if (!dataInRange.length) return;
    return (
      dataInRange.find(r => r.offset <= measure && measure < r.measure + r.offset) ||
      dataInRange[dataInRange.length - 1]
    );
  }

  const stackColorizer = useMemo(() => {
    const s = scaleOrdinal(COLORS);
    return (v: string) => (v === OTHER_VALUE ? OTHER_COLOR : s(v));
  }, []);

  const chartMargin = { ...margin, ...getDefaultChartMargin(yAxis) };
  const innerStage = stage.applyMargin(chartMargin);

  const effectiveDomainRange =
    domainRange ||
    (stackedData.length
      ? [stackedData[stackedData.length - 1].start, stackedData[0].end]
      : getTodayRange(Timezone.UTC));

  const baseTimeScale = scaleUtc()
    .domain(effectiveDomainRange)
    .range([EXTEND_X_SCALE_DOMAIN_BY, innerStage.width - EXTEND_X_SCALE_DOMAIN_BY]);
  const timeScale = shiftOffset
    ? baseTimeScale.copy().domain(offsetRange(effectiveDomainRange, shiftOffset))
    : baseTimeScale;

  const maxMeasure = max(stackedData, d => d.measure + d.offset);
  const measureScale = scaleLinear()
    .rangeRound([innerStage.height, 0])
    .domain([0, (maxMeasure ?? 100) * 1.05]);

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
    const action = y > innerStage.height || e.shiftKey ? 'shift' : 'select';
    setMouseDownAt({
      time,
      action,
    });
    if (action === 'select') {
      const start = granularity.floor(new Date(time), Timezone.UTC);
      setSelectionIfNeeded({
        start: start.valueOf(),
        end: granularity.shift(start, Timezone.UTC, 1).valueOf(),
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
        if (mouseDownAt.time < b) {
          setSelectionIfNeeded({
            start: granularity.floor(new Date(mouseDownAt.time), Timezone.UTC).valueOf(),
            end: granularity.ceil(new Date(b), Timezone.UTC).valueOf(),
          });
        } else {
          setSelectionIfNeeded({
            start: granularity.floor(new Date(b), Timezone.UTC).valueOf(),
            end: granularity.ceil(new Date(mouseDownAt.time), Timezone.UTC).valueOf(),
          });
        }
      }
    } else if (!selection?.finalized) {
      if (
        0 <= x &&
        x <= innerStage.width &&
        0 <= y &&
        y <= innerStage.height &&
        svg.contains(e.target as any)
      ) {
        const time = baseTimeScale.invert(x).valueOf();
        const measure = measureScale.invert(y);

        const start = granularity.floor(new Date(time), Timezone.UTC);
        const end = granularity.shift(start, Timezone.UTC, 1);

        setSelectionIfNeeded({
          start: start.valueOf(),
          end: end.valueOf(),
          selectedDatum: findStackedDatum(time, measure),
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
            snapGranularity.round(new Date(n), Timezone.UTC).valueOf(),
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

  const byStack = useMemo(() => {
    if (markType === 'bar' || !stackedData.length) return [];

    const effectiveStacks = stacks || ['undefined'];
    const numStacks = effectiveStacks.length;
    if (numStacks === 1) return [stackedData];

    // Fill in 0s and make sure that the stacks are in the same order
    const fullTimeIntervals = groupBy(
      stackedData,
      d => String(d.start),
      dataForStart => {
        const stackToDatum = lookupBy(dataForStart, d => d.stack!);
        return effectiveStacks.map(
          (stack, stackIndex) =>
            stackToDatum[stack] || {
              ...dataForStart[0],
              stack,
              measure: 0,
              offset: Math.max(
                0,
                ...filterMap(effectiveStacks.slice(0, stackIndex), s => stackToDatum[s]).map(
                  d => d.offset + d.measure,
                ),
              ),
            },
        );
      },
    );

    // Add nulls to mark gaps in data
    const seriesForStack: Record<string, (StackedRangeDatum | null)[]> = {};
    for (const stack of effectiveStacks) {
      seriesForStack[stack] = [];
    }

    let lastDatum: StackedRangeDatum | undefined;
    for (const fullTimeInterval of fullTimeIntervals) {
      const datum = fullTimeInterval[0];

      if (lastDatum && lastDatum.start !== datum.end) {
        for (const stack of effectiveStacks) {
          seriesForStack[stack].push(null);
        }
      }

      for (let i = 0; i < numStacks; i++) {
        seriesForStack[effectiveStacks[i]].push(fullTimeInterval[i]);
      }
      lastDatum = datum;
    }

    return Object.values(seriesForStack);
  }, [markType, stackedData, stacks]);

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

  function datumToYHeight({ measure, offset }: StackedRangeDatum) {
    const y0 = measureScale(offset);
    const y = measureScale(measure + offset);

    return {
      y: y,
      height: y0 - y,
    };
  }

  function datumToRect(d: StackedRangeDatum) {
    const xWidth = startEndToXWidth(d);
    if (!xWidth) return;
    return {
      ...xWidth,
      ...datumToYHeight(d),
    };
  }

  function datumToCxCy(d: StackedRangeDatum) {
    const cx = timeScale((d.start + d.end) / 2);
    if (cx < 0 || innerStage.width < cx) return;

    return {
      cx,
      cy: measureScale(d.measure + d.offset),
    };
  }

  const curve = getCurveFactory(curveType);

  const areaFn = area<StackedRangeDatum>()
    .curve(curve)
    .defined(Boolean)
    .x(d => timeScale((d.start + d.end) / 2))
    .y0(d => measureScale(d.offset))
    .y1(d => measureScale(d.measure + d.offset)) as Area<StackedRangeDatum | null>;

  const lineFn = line<StackedRangeDatum>()
    .curve(curve)
    .defined(Boolean)
    .x(d => timeScale((d.start + d.end) / 2))
    .y(d => measureScale(d.measure + d.offset)) as Line<StackedRangeDatum | null>;

  let hoveredOpenOn: PortalBubbleOpenOn | undefined;
  if (selection) {
    const { start, end, selectedDatum } = selection;

    let title: string;
    let info: string;
    if (selectedDatum) {
      title = formatStartDuration(new Date(selectedDatum.start), granularity);
      info = formatNumber(selectedDatum.measure);
    } else {
      if (granularity.shift(new Date(start), Timezone.UTC).valueOf() === end) {
        title = formatStartDuration(new Date(start), granularity);
      } else {
        title = formatIsoDateRange(new Date(start), new Date(end));
      }

      const selectedData = stackedData.filter(d => start <= d.start && d.start < end);
      if (selectedData.length) {
        info = formatNumber(sum(selectedData, b => b.measure));
      } else {
        info = 'No data';
      }
    }

    hoveredOpenOn = {
      x: chartMargin.left + timeScale((selection.start + selection.end) / 2),
      y: chartMargin.top,
      title,
      text: (
        <>
          {selectedDatum?.stack && <div>{selectedDatum?.stack}</div>}
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
  const nowDayCeil = day.ceil(now, Timezone.UTC);
  const zoomedOutRange: Range = [
    shiftStartBack,
    shiftEndForward < nowDayCeil.valueOf() ? shiftEndForward : nowDayCeil.valueOf(),
  ];

  const nowX = timeScale(now);
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
          {gridlinesVisible && (
            <g className="h-gridline" transform="translate(0,0)">
              {filterMap(measureScale.ticks(3), (v, i) => {
                if (v === 0) return;
                const y = measureScale(v);
                return <line key={i} x1={0} y1={y} x2={innerStage.width} y2={y} />;
              })}
            </g>
          )}
          <g clipPath={`xywh(0px 0px ${innerStage.width}px ${innerStage.height}px) view-box`}>
            {selection && (
              <rect
                className={classNames('selection', { finalized: selection.finalized })}
                {...startEndToXWidth(selection)}
                y={0}
                height={innerStage.height}
              />
            )}
            {0 < nowX && nowX < innerStage.width && (
              <line className="now-line" x1={nowX} x2={nowX} y1={0} y2={innerStage.height + 8} />
            )}
            {markType === 'bar' &&
              filterMap(stackedData, stackedRow => {
                const r = datumToRect(stackedRow);
                if (!r) return;
                return (
                  <rect
                    key={`${stackedRow.start}/${stackedRow.end}/${stackedRow.stack}`}
                    className="mark-bar"
                    {...r}
                    style={
                      typeof stackedRow.stack !== 'undefined'
                        ? {
                            fill: stackColorizer(stackedRow.stack),
                          }
                        : undefined
                    }
                  />
                );
              })}
            {markType === 'bar' && selection?.selectedDatum && (
              <rect
                className={classNames('selected-bar', { finalized: selection.finalized })}
                {...datumToRect(selection.selectedDatum)}
              />
            )}
            {markType === 'area' &&
              byStack.map(ds => {
                const stack = ds[0]!.stack;
                return (
                  <path
                    key={String(stack)}
                    className="mark-area"
                    d={areaFn(ds)!}
                    style={
                      typeof stack !== 'undefined'
                        ? {
                            fill: stackColorizer(stack),
                          }
                        : undefined
                    }
                  />
                );
              })}
            {(markType === 'area' || markType === 'line') &&
              byStack.map(ds => {
                const stack = ds[0]!.stack;
                return (
                  <path
                    key={String(stack)}
                    className="mark-line"
                    d={lineFn(ds)!}
                    style={
                      typeof stack !== 'undefined'
                        ? {
                            stroke: stackColorizer(stack),
                          }
                        : undefined
                    }
                  />
                );
              })}
            {(markType === 'area' || markType === 'line') && selection?.selectedDatum && (
              <circle
                className={classNames('selected-point', { finalized: selection.finalized })}
                {...datumToCxCy(selection.selectedDatum)}
                r={3}
                style={
                  typeof selection.selectedDatum.stack !== 'undefined'
                    ? {
                        fill: stackColorizer(selection.selectedDatum.stack),
                      }
                    : undefined
                }
              />
            )}
            {!!shiftOffset && (
              <rect
                className="shifter"
                x={shiftOffset > 0 ? timeScale(effectiveDomainRange[1]) : 0}
                y={0}
                height={innerStage.height}
                width={
                  shiftOffset > 0
                    ? innerStage.width - timeScale(effectiveDomainRange[1])
                    : timeScale(effectiveDomainRange[0])
                }
              />
            )}
          </g>
          <g
            className="axis-x"
            transform={`translate(0,${innerStage.height + 1})`}
            ref={(node: any) => select(node).call(axisBottom(timeScale))}
          />
          <rect
            className={classNames('time-shift-indicator', {
              shifting: typeof shiftOffset === 'number',
            })}
            x={0}
            y={innerStage.height}
            width={innerStage.width}
            height={chartMargin.bottom}
          />
          {yAxis === 'left' && (
            <g
              className="axis-y"
              ref={(node: any) =>
                select(node).call(
                  axisLeft(measureScale)
                    .ticks(3)
                    .tickFormat(e => formatNumber(e.valueOf())),
                )
              }
            />
          )}
          {yAxis === 'right' && (
            <g
              className="axis-y"
              transform={`translate(${innerStage.width},0)`}
              ref={(node: any) =>
                select(node).call(
                  axisRight(measureScale)
                    .ticks(3)
                    .tickFormat(e => formatNumber(e.valueOf())),
                )
              }
            />
          )}
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
