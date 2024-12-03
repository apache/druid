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
import type { NonNullDateRange } from '@blueprintjs/datetime';
import { IconNames } from '@blueprintjs/icons';
import IntervalTree from '@flatten-js/interval-tree';
import classNames from 'classnames';
import { max, sort, sum } from 'd3-array';
import { axisBottom, axisLeft } from 'd3-axis';
import { scaleLinear, scaleUtc } from 'd3-scale';
import type { MouseEvent as ReactMouseEvent, ReactNode } from 'react';
import { useMemo, useRef, useState } from 'react';

import type { Rule } from '../../druid-models';
import { getDatasourceColor, RuleUtil } from '../../druid-models';
import { useClock, useGlobalEventListener } from '../../hooks';
import {
  allSameValue,
  arraysEqualByElement,
  clamp,
  day,
  Duration,
  formatBytes,
  formatNumber,
  groupBy,
  groupByAsMap,
  minute,
  month,
  pluralIfNeeded,
  TZ_UTC,
  uniq,
} from '../../utils';
import type { Margin, Stage } from '../../utils/stage';
import type { PortalBubbleOpenOn } from '../portal-bubble/portal-bubble';
import { PortalBubble } from '../portal-bubble/portal-bubble';

import { ChartAxis } from './chart-axis';
import type { IntervalBar, IntervalRow, IntervalStat, TrimmedIntervalRow } from './common';
import { aggregateSegmentStats, formatIntervalStat, formatIsoDateOnly } from './common';

import './segment-bar-chart-render.scss';

const CHART_MARGIN: Margin = { top: 20, right: 0, bottom: 25, left: 70 };
const MIN_BAR_WIDTH = 4;
const POSSIBLE_GRANULARITIES = [
  new Duration('PT15M'),
  new Duration('PT1H'),
  new Duration('PT6H'),
  new Duration('P1D'),
  new Duration('P1M'),
  new Duration('P1Y'),
];

const EXTEND_X_SCALE_DOMAIN_BY = 1;

function formatStartDuration(start: Date, duration: Duration): string {
  let sliceLength;
  const { singleSpan } = duration;
  switch (singleSpan) {
    case 'year':
      sliceLength = 4;
      break;

    case 'month':
      sliceLength = 7;
      break;

    case 'day':
      sliceLength = 10;
      break;

    case 'hour':
      sliceLength = 13;
      break;

    case 'minute':
      sliceLength = 16;
      break;

    default:
      sliceLength = 19;
      break;
  }

  return `${start.toISOString().slice(0, sliceLength)}/${duration}`;
}

// ---------------------------------------
// Load rule stuff

function loadRuleToBaseType(loadRule: Rule): string {
  const m = /^(load|drop|broadcast)/.exec(loadRule.type);
  return m ? m[1] : 'load';
}

const NEGATIVE_INFINITY_DATE = new Date(Date.UTC(1000, 0, 1));
const POSITIVE_INFINITY_DATE = new Date(Date.UTC(3000, 0, 1));

function loadRuleToDateRange(loadRule: Rule): NonNullDateRange {
  switch (loadRule.type) {
    case 'loadByInterval':
    case 'dropByInterval':
    case 'broadcastByInterval':
      return String(loadRule.interval)
        .split('/')
        .map(d => new Date(d)) as NonNullDateRange;

    case 'loadByPeriod':
    case 'dropByPeriod':
    case 'broadcastByPeriod':
      return [
        new Duration(loadRule.period || 'P1D').shift(new Date(), TZ_UTC, -1),
        loadRule.includeFuture ? POSITIVE_INFINITY_DATE : new Date(),
      ];

    case 'dropBeforeByPeriod':
      return [
        NEGATIVE_INFINITY_DATE,
        new Duration(loadRule.period || 'P1D').shift(new Date(), TZ_UTC, -1),
      ];

    default:
      return [NEGATIVE_INFINITY_DATE, POSITIVE_INFINITY_DATE];
  }
}

// ---------------------------------------

function offsetDateRange(dateRange: NonNullDateRange, offset: number): NonNullDateRange {
  return [new Date(dateRange[0].valueOf() + offset), new Date(dateRange[1].valueOf() + offset)];
}

function stackIntervalRows(trimmedIntervalRows: TrimmedIntervalRow[]): {
  intervalBars: IntervalBar[];
  intervalTree: IntervalTree;
} {
  // Total size of the datasource will be used as an ordering tiebreaker
  const datasourceToTotalSize = groupByAsMap(
    trimmedIntervalRows,
    intervalRow => intervalRow.datasource,
    intervalRows => sum(intervalRows, intervalRow => intervalRow.size),
  );

  const sortedIntervalRows = sort(trimmedIntervalRows, (a, b) => {
    const shownDaysDiff = b.shownDays - a.shownDays;
    if (shownDaysDiff) return shownDaysDiff;

    const timeSpanDiff =
      b.originalTimeSpan.getCanonicalLength() - a.originalTimeSpan.getCanonicalLength();
    if (timeSpanDiff) return timeSpanDiff;

    const totalSizeDiff = datasourceToTotalSize[b.datasource] - datasourceToTotalSize[a.datasource];
    if (totalSizeDiff) return totalSizeDiff;

    return Number(a.realtime) - Number(b.realtime);
  });

  const intervalTree = new IntervalTree();
  const intervalBars = sortedIntervalRows.map(intervalRow => {
    const startMs = intervalRow.start.valueOf();
    const endMs = intervalRow.end.valueOf();
    const intervalRowsBelow = intervalTree.search([startMs + 1, startMs + 2]) as IntervalBar[];
    const intervalBar: IntervalBar = {
      ...intervalRow,
      offset: aggregateSegmentStats(intervalRowsBelow.map(i => i.normalized)),
    };
    intervalTree.insert([startMs, endMs], intervalBar);
    return intervalBar;
  });

  return {
    intervalBars,
    intervalTree,
  };
}

interface BubbleInfo {
  start: Date;
  end: Date;
  timeLabel: string;
  intervalBars: IntervalBar[];
}

interface SelectionRange {
  start: Date;
  end: Date;
  done?: boolean;
}

export interface DatasourceRules {
  loadRules: Rule[];
  defaultLoadRules: Rule[];
}

export interface SegmentBarChartRenderProps {
  intervalRows: IntervalRow[];
  datasourceRules: DatasourceRules | undefined;
  datasourceRulesError: string | undefined;

  stage: Stage;
  dateRange: NonNullDateRange;
  changeDateRange(dateRange: NonNullDateRange): void;
  shownIntervalStat: IntervalStat;
  shownDatasource: string | undefined;
  changeShownDatasource(datasource: string | undefined): void;
  getIntervalActionButton?(
    start: Date,
    end: Date,
    datasource?: string,
    realtime?: boolean,
  ): ReactNode;
}

export const SegmentBarChartRender = function SegmentBarChartRender(
  props: SegmentBarChartRenderProps,
) {
  const {
    intervalRows,
    datasourceRules,
    datasourceRulesError,

    stage,
    shownIntervalStat,
    dateRange,
    changeDateRange,
    shownDatasource,
    changeShownDatasource,
    getIntervalActionButton,
  } = props;
  const [mouseDownAt, setMouseDownAt] = useState<
    { time: Date; action: 'select' | 'shift' } | undefined
  >();
  const [selection, setSelection] = useState<SelectionRange | undefined>();

  function setSelectionIfNeeded(newSelection: SelectionRange) {
    if (
      selection &&
      selection.start.valueOf() === newSelection.start.valueOf() &&
      selection.end.valueOf() === newSelection.end.valueOf() &&
      selection.done === newSelection.done
    ) {
      return;
    }
    setSelection(newSelection);
  }

  const [bubbleInfo, setBubbleInfo] = useState<BubbleInfo | undefined>();

  function setBubbleInfoIfNeeded(newBubbleInfo: BubbleInfo) {
    if (
      bubbleInfo &&
      bubbleInfo.start.valueOf() === newBubbleInfo.start.valueOf() &&
      bubbleInfo.end.valueOf() === newBubbleInfo.end.valueOf() &&
      bubbleInfo.timeLabel === newBubbleInfo.timeLabel &&
      arraysEqualByElement(bubbleInfo.intervalBars, newBubbleInfo.intervalBars)
    ) {
      return;
    }
    setBubbleInfo(newBubbleInfo);
  }

  const [shiftOffset, setShiftOffset] = useState<number | undefined>();

  const now = useClock(minute.canonicalLength);
  const svgRef = useRef<SVGSVGElement | null>(null);

  const trimGranularity = useMemo(() => {
    return Duration.pickSmallestGranularityThatFits(
      POSSIBLE_GRANULARITIES,
      dateRange[1].valueOf() - dateRange[0].valueOf(),
      Math.floor(stage.width / MIN_BAR_WIDTH),
    ).toString();
  }, [dateRange, stage.width]);

  const { intervalBars, intervalTree } = useMemo(() => {
    const shownIntervalRows = intervalRows.filter(
      ({ start, end, datasource }) =>
        start <= dateRange[1] &&
        dateRange[0] < end &&
        (!shownDatasource || datasource === shownDatasource),
    );
    const averageRowSizeByDatasource = groupByAsMap(
      shownIntervalRows.filter(intervalRow => intervalRow.size > 0 && intervalRow.rows > 0),
      intervalRow => intervalRow.datasource,
      intervalRows => sum(intervalRows, d => d.size) / sum(intervalRows, d => d.rows),
    );

    const trimDuration = new Duration(trimGranularity);
    const trimmedIntervalRows = shownIntervalRows.map(intervalRow => {
      const { start, end, segments, size, rows } = intervalRow;
      const startTrimmed = trimDuration.floor(start, TZ_UTC);
      let endTrimmed = trimDuration.ceil(end, TZ_UTC);

      // Special handling to catch WEEK intervals when trimming to month.
      if (trimGranularity === 'P1M' && intervalRow.originalTimeSpan.toString() === 'P7D') {
        endTrimmed = trimDuration.shift(startTrimmed, TZ_UTC);
      }

      const shownDays = (endTrimmed.valueOf() - startTrimmed.valueOf()) / day.canonicalLength;
      const shownSize =
        size === 0 ? rows * averageRowSizeByDatasource[intervalRow.datasource] : size;
      return {
        ...intervalRow,
        start: startTrimmed,
        end: endTrimmed,
        shownDays,
        size: shownSize,
        normalized: {
          size: shownSize / shownDays,
          rows: rows / shownDays,
          segments: segments / shownDays,
        },
      };
    });

    const fullyGroupedSegmentRows = groupBy(
      trimmedIntervalRows,
      trimmedIntervalRow =>
        [
          trimmedIntervalRow.start.toISOString(),
          trimmedIntervalRow.end.toISOString(),
          trimmedIntervalRow.originalTimeSpan,
          trimmedIntervalRow.datasource,
          trimmedIntervalRow.realtime,
        ].join('/'),
      (trimmedIntervalRows): TrimmedIntervalRow => {
        const firstIntervalRow = trimmedIntervalRows[0];
        return {
          ...firstIntervalRow,
          ...aggregateSegmentStats(trimmedIntervalRows),
          normalized: aggregateSegmentStats(trimmedIntervalRows.map(t => t.normalized)),
        };
      },
    );

    return stackIntervalRows(fullyGroupedSegmentRows);
  }, [intervalRows, trimGranularity, dateRange, shownDatasource]);

  const innerStage = stage.applyMargin(CHART_MARGIN);

  const baseTimeScale = scaleUtc()
    .domain(dateRange)
    .range([EXTEND_X_SCALE_DOMAIN_BY, innerStage.width - EXTEND_X_SCALE_DOMAIN_BY]);
  const timeScale = shiftOffset
    ? baseTimeScale.copy().domain(offsetDateRange(dateRange, shiftOffset))
    : baseTimeScale;

  const maxNormalizedStat = max(
    intervalBars,
    d => d.normalized[shownIntervalStat] + d.offset[shownIntervalStat],
  );
  const statScale = scaleLinear()
    .rangeRound([innerStage.height, 0])
    .domain([0, (maxNormalizedStat ?? 1) * 1.05]);

  const formatTickRate = (n: number) => {
    switch (shownIntervalStat) {
      case 'segments':
        return formatNumber(n); // + ' seg/day';

      case 'rows':
        return formatNumber(n); // + ' row/day';

      case 'size':
        return formatBytes(n);
    }
  };

  function handleMouseDown(e: ReactMouseEvent) {
    const svg = svgRef.current;
    if (!svg) return;
    e.preventDefault();

    if (selection) {
      setSelection(undefined);
    } else {
      const rect = svg.getBoundingClientRect();
      const x = e.clientX - rect.x - CHART_MARGIN.left;
      const y = e.clientY - rect.y - CHART_MARGIN.top;
      const time = baseTimeScale.invert(x);
      const action = y > innerStage.height || e.shiftKey ? 'shift' : 'select';
      setBubbleInfo(undefined);
      setMouseDownAt({
        time,
        action,
      });
    }
  }

  useGlobalEventListener('mousemove', (e: MouseEvent) => {
    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const x = e.clientX - rect.x - CHART_MARGIN.left;
    const y = e.clientY - rect.y - CHART_MARGIN.top;

    if (mouseDownAt) {
      e.preventDefault();

      const b = baseTimeScale.invert(x);
      if (mouseDownAt.action === 'shift' || e.shiftKey) {
        setShiftOffset(mouseDownAt.time.valueOf() - b.valueOf());
      } else {
        if (mouseDownAt.time < b) {
          setSelectionIfNeeded({
            start: day.floor(mouseDownAt.time, TZ_UTC),
            end: day.ceil(b, TZ_UTC),
          });
        } else {
          setSelectionIfNeeded({
            start: day.floor(b, TZ_UTC),
            end: day.ceil(mouseDownAt.time, TZ_UTC),
          });
        }
      }
    } else if (!selection) {
      if (
        0 <= x &&
        x <= innerStage.width &&
        0 <= y &&
        y <= innerStage.height + CHART_MARGIN.bottom
      ) {
        const time = baseTimeScale.invert(x);
        const shifter =
          new Duration(trimGranularity).getCanonicalLength() > day.canonicalLength * 25
            ? month
            : day;
        const start = shifter.floor(time, TZ_UTC);
        const end = shifter.ceil(time, TZ_UTC);

        let intervalBars: IntervalBar[] = [];
        if (y <= innerStage.height) {
          const bars = intervalTree.search([
            time.valueOf() + 1,
            time.valueOf() + 2,
          ]) as IntervalBar[];

          if (bars.length) {
            const stat = statScale.invert(y);
            const hoverBar = bars.find(
              bar =>
                bar.offset[shownIntervalStat] <= stat &&
                stat < bar.offset[shownIntervalStat] + bar.normalized[shownIntervalStat],
            );
            intervalBars = hoverBar ? [hoverBar] : bars;
          }
        }
        setBubbleInfoIfNeeded({
          start,
          end,
          timeLabel: start.toISOString().slice(0, shifter === day ? 10 : 7),
          intervalBars,
        });
      } else {
        setBubbleInfo(undefined);
      }
    }
  });

  useGlobalEventListener('mouseup', (e: MouseEvent) => {
    if (!mouseDownAt) return;
    e.preventDefault();
    setMouseDownAt(undefined);

    const svg = svgRef.current;
    if (!svg) return;
    const rect = svg.getBoundingClientRect();
    const x = e.clientX - rect.x - CHART_MARGIN.left;
    const y = e.clientY - rect.y - CHART_MARGIN.top;

    if (shiftOffset || selection) {
      setShiftOffset(undefined);
      if (mouseDownAt.action === 'shift' || e.shiftKey) {
        if (shiftOffset) {
          changeDateRange(offsetDateRange(dateRange, shiftOffset));
        }
      } else {
        if (selection) {
          setSelection({ ...selection, done: true });
        }
      }
    } else if (0 <= x && x <= innerStage.width && 0 <= y && y <= innerStage.height) {
      const time = baseTimeScale.invert(x);

      const bars = intervalTree.search([time.valueOf() + 1, time.valueOf() + 2]) as IntervalBar[];

      if (bars.length) {
        const stat = statScale.invert(y);
        const hoverBar = bars.find(
          bar =>
            bar.offset[shownIntervalStat] <= stat &&
            stat < bar.offset[shownIntervalStat] + bar.normalized[shownIntervalStat],
        );
        if (hoverBar) {
          changeShownDatasource(shownDatasource ? undefined : hoverBar.datasource);
        }
      }
    }
  });

  useGlobalEventListener('keydown', (e: KeyboardEvent) => {
    if (e.key === 'Escape' && mouseDownAt) {
      setMouseDownAt(undefined);
      setSelection(undefined);
    }
  });

  function startEndToXWidth({ start, end }: { start: Date; end: Date }) {
    const xStart = clamp(timeScale(start), 0, innerStage.width);
    const xEnd = clamp(timeScale(end), 0, innerStage.width);

    return {
      x: xStart,
      width: Math.max(xEnd - xStart - 1, 1),
    };
  }

  function segmentBarToRect(intervalBar: IntervalBar) {
    const y0 = statScale(intervalBar.offset[shownIntervalStat]);
    const y = statScale(
      intervalBar.normalized[shownIntervalStat] + intervalBar.offset[shownIntervalStat],
    );

    return {
      ...startEndToXWidth(intervalBar),
      y: y,
      height: y0 - y,
    };
  }

  let hoveredOpenOn: PortalBubbleOpenOn | undefined;
  if (svgRef.current) {
    const rect = svgRef.current.getBoundingClientRect();

    if (bubbleInfo) {
      const hoveredIntervalBars = bubbleInfo.intervalBars;

      let title: string | undefined;
      let text: ReactNode;
      if (hoveredIntervalBars.length === 0) {
        title = bubbleInfo.timeLabel;
        text = '';
      } else if (hoveredIntervalBars.length === 1) {
        const hoveredIntervalBar = hoveredIntervalBars[0];
        title = `${formatStartDuration(
          hoveredIntervalBar.start,
          hoveredIntervalBar.originalTimeSpan,
        )}${hoveredIntervalBar.realtime ? ' (realtime)' : ''}`;
        text = (
          <>
            {!shownDatasource && <div>{`Datasource: ${hoveredIntervalBar.datasource}`}</div>}
            <div>{`Size: ${
              hoveredIntervalBar.realtime
                ? 'estimated for realtime'
                : formatIntervalStat('size', hoveredIntervalBar.size)
            }`}</div>
            <div>{`Rows: ${formatIntervalStat('rows', hoveredIntervalBar.rows)}`}</div>
            <div>{`Segments: ${formatIntervalStat('segments', hoveredIntervalBar.segments)}`}</div>
          </>
        );
      } else {
        const datasources = uniq(hoveredIntervalBars.map(b => b.datasource));
        const agg = aggregateSegmentStats(hoveredIntervalBars);
        title = bubbleInfo.timeLabel;
        text = (
          <>
            {!shownDatasource && (
              <div>{`Totals for ${pluralIfNeeded(datasources.length, 'datasource')}`}</div>
            )}
            <div>{`Size: ${formatIntervalStat('size', agg.size)}`}</div>
            <div>{`Rows: ${formatIntervalStat('rows', agg.rows)}`}</div>
            <div>{`Segments: ${formatIntervalStat('segments', agg.segments)}`}</div>
          </>
        );
      }

      hoveredOpenOn = {
        x:
          rect.x +
          CHART_MARGIN.left +
          timeScale(new Date((bubbleInfo.start.valueOf() + bubbleInfo.end.valueOf()) / 2)),
        y: rect.y + CHART_MARGIN.top,
        title,
        text,
      };
    } else if (selection) {
      const selectedBars = intervalTree.search([
        selection.start.valueOf() + 1,
        selection.end.valueOf() - 1,
      ]) as IntervalBar[];
      const datasources = uniq(selectedBars.map(b => b.datasource));
      const realtime = allSameValue(selectedBars.map(b => b.realtime));
      const agg = aggregateSegmentStats(selectedBars);
      hoveredOpenOn = {
        x:
          rect.x +
          CHART_MARGIN.left +
          timeScale(new Date((selection.start.valueOf() + selection.end.valueOf()) / 2)),
        y: rect.y + CHART_MARGIN.top,
        title: `${formatIsoDateOnly(selection.start)} → ${formatIsoDateOnly(selection.end)}`,
        text: (
          <>
            {selectedBars.length ? (
              <>
                {!shownDatasource && (
                  <div>{`Totals for ${pluralIfNeeded(datasources.length, 'datasource')}`}</div>
                )}
                <div>{`Size: ${formatIntervalStat('size', agg.size)}`}</div>
                <div>{`Rows: ${formatIntervalStat('rows', agg.rows)}`}</div>
                <div>{`Segments: ${formatIntervalStat('segments', agg.segments)}`}</div>
              </>
            ) : (
              <div>No segments in this interval</div>
            )}
            {selection.done && (
              <div className="button-bar">
                <Button
                  icon={IconNames.ZOOM_IN}
                  text="Zoom in"
                  intent={Intent.PRIMARY}
                  small
                  onClick={() => {
                    if (!selection) return;
                    setSelection(undefined);
                    changeDateRange([selection.start, selection.end]);
                  }}
                />
                {getIntervalActionButton?.(
                  selection.start,
                  selection.end,
                  datasources.length === 1 ? datasources[0] : undefined,
                  realtime,
                )}
              </div>
            )}
          </>
        ),
      };
    }
  }

  function renderLoadRule(loadRule: Rule, i: number, isDefault: boolean) {
    const [start, end] = loadRuleToDateRange(loadRule);
    const { x, width } = startEndToXWidth({ start, end });
    const title = RuleUtil.ruleToString(loadRule) + (isDefault ? ' (cluster default)' : '');
    return (
      <div
        key={i}
        className={classNames('load-rule', loadRuleToBaseType(loadRule))}
        data-tooltip={title}
        style={{
          left: x,
          width,
        }}
      >
        {title}
      </div>
    );
  }

  const nowX = timeScale(now);
  return (
    <div className="segment-bar-chart-render">
      <svg
        ref={svgRef}
        width={stage.width}
        height={stage.height}
        viewBox={`0 0 ${stage.width} ${stage.height}`}
        preserveAspectRatio="xMinYMin meet"
        onMouseDown={handleMouseDown}
      >
        <g transform={`translate(${CHART_MARGIN.left},${CHART_MARGIN.top})`}>
          <ChartAxis
            className="gridline-x"
            transform="translate(0,0)"
            axis={axisLeft(statScale)
              .tickValues(statScale.ticks(3).filter(v => v !== 0))
              .tickSize(-innerStage.width)
              .tickFormat(() => '')
              .tickSizeOuter(0)}
          />
          <ChartAxis
            className="axis-x"
            transform={`translate(0,${innerStage.height})`}
            axis={axisBottom(timeScale)}
          />
          <rect
            className={classNames('time-shift-indicator', {
              shifting: typeof shiftOffset === 'number',
            })}
            x={0}
            y={innerStage.height}
            width={innerStage.width}
            height={CHART_MARGIN.bottom}
          />
          <ChartAxis
            className="axis-y"
            axis={axisLeft(statScale)
              .ticks(3)
              .tickFormat(e => formatTickRate(e.valueOf()))}
          />
          <g className="bar-group">
            {bubbleInfo && (
              <rect
                className="hover-highlight"
                {...startEndToXWidth(bubbleInfo)}
                y={0}
                height={innerStage.height}
              />
            )}
            {0 < nowX && nowX < innerStage.width && (
              <line className="now-line" x1={nowX} x2={nowX} y1={0} y2={innerStage.height + 8} />
            )}
            {intervalBars.map((intervalBar, i) => {
              return (
                <rect
                  key={i}
                  className={classNames('bar-unit', { realtime: intervalBar.realtime })}
                  {...segmentBarToRect(intervalBar)}
                  fill={getDatasourceColor(intervalBar.datasource)}
                />
              );
            })}
            {bubbleInfo?.intervalBars.length === 1 &&
              bubbleInfo.intervalBars.map((intervalBar, i) => (
                <rect key={i} className="hovered-bar" {...segmentBarToRect(intervalBar)} />
              ))}
            {selection && (
              <rect
                className={classNames('selection', { done: selection.done })}
                {...startEndToXWidth(selection)}
                y={0}
                height={innerStage.height}
              />
            )}
            {!!shiftOffset && (
              <rect
                className="shifter"
                x={shiftOffset > 0 ? timeScale(dateRange[1]) : 0}
                y={0}
                height={innerStage.height}
                width={
                  shiftOffset > 0
                    ? innerStage.width - timeScale(dateRange[1])
                    : timeScale(dateRange[0])
                }
              />
            )}
          </g>
        </g>
      </svg>
      {(datasourceRules || datasourceRulesError) && (
        <div className="rule-tape" style={{ left: CHART_MARGIN.left, right: CHART_MARGIN.right }}>
          {datasourceRules?.defaultLoadRules.map((rule, index) =>
            renderLoadRule(rule, index, true),
          )}
          {datasourceRules?.loadRules.map((rule, index) => renderLoadRule(rule, index, false))}
          {datasourceRulesError && (
            <div className="rule-error">Rule loading error: {datasourceRulesError}</div>
          )}
        </div>
      )}
      {!intervalRows.length && (
        <div className="empty-placeholder">
          <div className="no-data-text">There are no segments in the selected range</div>
        </div>
      )}
      <PortalBubble
        className="segment-bar-chart-bubble"
        openOn={hoveredOpenOn}
        onClose={selection?.done ? () => setSelection(undefined) : undefined}
        mute
        direction="up"
      />
    </div>
  );
};
