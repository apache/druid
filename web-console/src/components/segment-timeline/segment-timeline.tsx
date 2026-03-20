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
  ButtonGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Position,
  ResizeSensor,
} from '@blueprintjs/core';
import type { NonNullDateRange } from '@blueprintjs/datetime';
import { DateRangePicker3 } from '@blueprintjs/datetime2';
import { IconNames } from '@blueprintjs/icons';
import { Select } from '@blueprintjs/select';
import { day, Duration, Timezone } from 'chronoshift';
import { C, L, N, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import { useEffect, useMemo, useState } from 'react';

import type { Capabilities } from '../../helpers';
import { useQueryManager } from '../../hooks';
import {
  checkedCircleIcon,
  getApiArray,
  isNonNullRange,
  localToUtcDateRange,
  maxDate,
  queryDruidSql,
  Stage,
  utcToLocalDateRange,
} from '../../utils';
import { Loader } from '../loader/loader';

import type { IntervalStat } from './interval';
import { formatIsoDateOnly, getIntervalStatTitle, INTERVAL_STATS } from './interval';
import type { SegmentBarChartProps } from './segment-bar-chart';
import { SegmentBarChart } from './segment-bar-chart';

import './segment-timeline.scss';

const FOUR_DIGIT_YEAR_LIKE = '____-%';
const DEFAULT_SHOWN_DURATION = new Duration('P1Y');
const SHOWN_DURATION_OPTIONS: Duration[] = ['P1D', 'P1W', 'P1M', 'P3M', 'P1Y', 'P5Y', 'P10Y'].map(
  d => new Duration(d),
);

function getDateRange(shownDuration: Duration): NonNullDateRange {
  const end = day.ceil(new Date(), Timezone.UTC);
  return [shownDuration.shift(end, Timezone.UTC, -1), end];
}

function formatDateRange(dateRange: NonNullDateRange): string {
  return `${formatIsoDateOnly(dateRange[0])} → ${formatIsoDateOnly(dateRange[1])}`;
}

function dateRangesEqual(dr1: NonNullDateRange, dr2: NonNullDateRange): boolean {
  return dr1[0].valueOf() === dr2[0].valueOf() && dr2[1].valueOf() === dr2[1].valueOf();
}

interface SegmentTimelineProps extends Pick<SegmentBarChartProps, 'getIntervalActionButton'> {
  capabilities: Capabilities;
  datasource: string | undefined;
}

export const SegmentTimeline = function SegmentTimeline(props: SegmentTimelineProps) {
  const { capabilities, datasource, ...otherProps } = props;
  const [stage, setStage] = useState<Stage | undefined>();
  const [activeSegmentStat, setActiveSegmentStat] = useState<IntervalStat>('size');
  const [shownDatasource, setShownDatasource] = useState<string | undefined>(datasource);
  const [dateRange, setDateRange] = useState<NonNullDateRange | undefined>();
  const [showCustomDatePicker, setShowCustomDatePicker] = useState(false);

  useEffect(() => {
    setShownDatasource(datasource);
  }, [datasource]);

  const defaultDateRange = useMemo(() => {
    return getDateRange(DEFAULT_SHOWN_DURATION);
  }, []);

  const [datasourcesState] = useQueryManager<Capabilities, string[]>({
    initQuery: capabilities,
    processQuery: async (capabilities, signal) => {
      if (capabilities.hasSql()) {
        const tables = await queryDruidSql<{ TABLE_NAME: string }>(
          {
            query: `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'`,
            context: { engine: 'native' },
          },
          signal,
        );

        return tables.map(d => d.TABLE_NAME);
      } else {
        return await getApiArray(`/druid/coordinator/v1/datasources`, signal);
      }
    },
  });

  const [initDatasourceDateRangeState] = useQueryManager<string | null, NonNullDateRange>({
    query: dateRange ? undefined : shownDatasource ?? null,
    processQuery: async (datasource, signal) => {
      let queriedStart: Date;
      let queriedEnd: Date;
      if (capabilities.hasSql()) {
        const baseQuery = SqlQuery.from(N('sys').table('segments'))
          .changeWhereExpression(
            SqlExpression.and(
              C('start').like(FOUR_DIGIT_YEAR_LIKE),
              C('end').like(FOUR_DIGIT_YEAR_LIKE),
              C('is_overshadowed').equal(0),
              datasource ? C('datasource').equal(L(datasource)) : undefined,
            ),
          )
          .changeLimitValue(1);

        const endQuery = baseQuery
          .addSelect(C('end'), { addToOrderBy: 'end', direction: 'DESC' })
          .toString();

        const endRes = await queryDruidSql<{ end: string }>(
          { query: endQuery, context: { engine: 'native' } },
          signal,
        ).catch(() => []);
        if (endRes.length !== 1) {
          return getDateRange(DEFAULT_SHOWN_DURATION);
        }

        const endResDate = new Date(endRes[0].end); // Need to be protective against a date in the far future
        if (isNaN(endResDate.valueOf())) {
          return getDateRange(DEFAULT_SHOWN_DURATION);
        }

        queriedEnd = day.ceil(endResDate, Timezone.UTC);

        const startQuery = baseQuery
          .addSelect(C('start'), { addToOrderBy: 'end', direction: 'ASC' })
          .toString();

        const startRes = await queryDruidSql<{ start: string }>(
          { query: startQuery, context: { engine: 'native' } },
          signal,
        ).catch(() => []);
        if (startRes.length !== 1) {
          return [DEFAULT_SHOWN_DURATION.shift(queriedEnd, Timezone.UTC, -1), queriedEnd]; // Should not really get here
        }

        queriedStart = day.floor(new Date(startRes[0].start), Timezone.UTC);
      } else {
        // Don't bother querying if there is no SQL
        return getDateRange(DEFAULT_SHOWN_DURATION);
      }

      return [
        maxDate(queriedStart, DEFAULT_SHOWN_DURATION.shift(queriedEnd, Timezone.UTC, -1)),
        queriedEnd,
      ];
    },
  });

  const effectiveDateRange =
    dateRange ||
    initDatasourceDateRangeState.data ||
    (initDatasourceDateRangeState.isLoading() ? undefined : defaultDateRange);

  let previousDateRange: NonNullDateRange | undefined;
  let zoomedOutDateRange: NonNullDateRange | undefined;
  let nextDateRange: NonNullDateRange | undefined;
  if (effectiveDateRange) {
    const d = Duration.fromRange(effectiveDateRange[0], effectiveDateRange[1], Timezone.UTC);
    const shiftStartBack = d.shift(effectiveDateRange[0], Timezone.UTC, -1);
    const shiftEndForward = d.shift(effectiveDateRange[1], Timezone.UTC);
    const now = day.ceil(new Date(), Timezone.UTC);
    previousDateRange = [shiftStartBack, effectiveDateRange[0]];
    zoomedOutDateRange = [shiftStartBack, shiftEndForward < now ? shiftEndForward : now];
    nextDateRange = [effectiveDateRange[1], shiftEndForward];
  }

  return (
    <div className="segment-timeline">
      <div className="control-bar">
        <ButtonGroup>
          <Select<string>
            items={datasourcesState.data || []}
            disabled={datasourcesState.isError()}
            onItemSelect={setShownDatasource}
            itemRenderer={(val, { handleClick, handleFocus, modifiers }) => {
              if (!modifiers.matchesPredicate) return null;
              return (
                <MenuItem
                  key={val}
                  disabled={modifiers.disabled}
                  active={modifiers.active}
                  onClick={handleClick}
                  onFocus={handleFocus}
                  roleStructure="listoption"
                  text={val}
                />
              );
            }}
            noResults={<MenuItem disabled text="No results" roleStructure="listoption" />}
            itemPredicate={(query, val, _index, exactMatch) => {
              const normalizedTitle = val.toLowerCase();
              const normalizedQuery = query.toLowerCase();

              if (exactMatch) {
                return normalizedTitle === normalizedQuery;
              } else {
                return normalizedTitle.includes(normalizedQuery);
              }
            }}
          >
            <Button
              text={`Datasource: ${shownDatasource ?? 'all'}`}
              small
              rightIcon={IconNames.CARET_DOWN}
              intent={datasourcesState.isError() ? Intent.WARNING : undefined}
              data-tooltip={
                datasourcesState.isError()
                  ? `Error: ${datasourcesState.getErrorMessage()}`
                  : undefined
              }
            />
          </Select>
          {shownDatasource && (
            <Button icon={IconNames.CROSS} small onClick={() => setShownDatasource(undefined)} />
          )}
        </ButtonGroup>
        <Popover
          position={Position.BOTTOM_LEFT}
          content={
            <Menu>
              {INTERVAL_STATS.map(stat => (
                <MenuItem
                  key={stat}
                  icon={checkedCircleIcon(stat === activeSegmentStat)}
                  text={getIntervalStatTitle(stat)}
                  onClick={() => setActiveSegmentStat(stat)}
                />
              ))}
            </Menu>
          }
        >
          <Button
            text={`Show: ${getIntervalStatTitle(activeSegmentStat)}`}
            small
            rightIcon={IconNames.CARET_DOWN}
          />
        </Popover>
        <div className="expander" />
        <ButtonGroup>
          <Button
            icon={IconNames.CARET_LEFT}
            data-tooltip={
              previousDateRange && `Previous time period\n${formatDateRange(previousDateRange)}`
            }
            small
            disabled={!previousDateRange}
            onClick={() => setDateRange(previousDateRange)}
          />
          <Button
            icon={IconNames.ZOOM_OUT}
            data-tooltip={zoomedOutDateRange && `Zoom out\n${formatDateRange(zoomedOutDateRange)}`}
            small
            disabled={!zoomedOutDateRange}
            onClick={() => setDateRange(zoomedOutDateRange)}
          />
          <Button
            icon={IconNames.CARET_RIGHT}
            data-tooltip={nextDateRange && `Next time period\n${formatDateRange(nextDateRange)}`}
            small
            disabled={!nextDateRange}
            onClick={() => setDateRange(nextDateRange)}
          />
        </ButtonGroup>
        <ButtonGroup>
          {SHOWN_DURATION_OPTIONS.map((d, i) => {
            const dr = getDateRange(d);
            return (
              <Button
                key={i}
                text={d.toString().replace('P', '')}
                data-tooltip={`Show last ${d.getDescription()}\n${formatDateRange(dr)}`}
                active={effectiveDateRange && dateRangesEqual(effectiveDateRange, dr)}
                small
                onClick={() => setDateRange(dr)}
              />
            );
          })}
          <Popover
            isOpen={showCustomDatePicker}
            onInteraction={setShowCustomDatePicker}
            content={
              <DateRangePicker3
                defaultValue={utcToLocalDateRange(
                  effectiveDateRange || getDateRange(DEFAULT_SHOWN_DURATION),
                )}
                onChange={newDateRange => {
                  const newUtcDateRange = localToUtcDateRange(newDateRange);
                  if (!isNonNullRange(newUtcDateRange)) return;
                  setDateRange(newUtcDateRange);
                  setShowCustomDatePicker(false);
                }}
                contiguousCalendarMonths={false}
                reverseMonthAndYearMenus
                timePickerProps={undefined}
                shortcuts={false}
              />
            }
          >
            <Button
              icon={IconNames.CALENDAR}
              text={
                effectiveDateRange
                  ? formatDateRange(effectiveDateRange)
                  : `Loading datasource date range`
              }
              data-tooltip={showCustomDatePicker ? undefined : `Select a custom date range`}
              small
            />
          </Popover>
          <Button
            icon={IconNames.PIN}
            data-tooltip={
              dateRange ? 'Pin the date range' : 'Auto determine date range to fit datasource'
            }
            active={Boolean(dateRange)}
            disabled={!effectiveDateRange}
            small
            onClick={() => setDateRange(dateRange ? undefined : effectiveDateRange)}
          />
        </ButtonGroup>
      </div>
      <ResizeSensor
        onResize={(entries: ResizeObserverEntry[]) => {
          const rect = entries[0].contentRect;
          setStage(new Stage(rect.width, rect.height));
        }}
      >
        <div className="chart-container">
          {stage && effectiveDateRange && (
            <SegmentBarChart
              capabilities={capabilities}
              stage={stage}
              dateRange={effectiveDateRange}
              changeDateRange={setDateRange}
              shownIntervalStat={activeSegmentStat}
              shownDatasource={shownDatasource}
              changeShownDatasource={setShownDatasource}
              {...otherProps}
            />
          )}
          {initDatasourceDateRangeState.isLoading() && <Loader />}
        </div>
      </ResizeSensor>
    </div>
  );
};
