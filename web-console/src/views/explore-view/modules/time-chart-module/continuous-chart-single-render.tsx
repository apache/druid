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

import classNames from 'classnames';
import { axisLeft, axisRight } from 'd3-axis';
import type { ScaleLinear, ScaleTime } from 'd3-scale';
import { select } from 'd3-selection';
import type { Area, Line } from 'd3-shape';
import { area, curveLinear, curveMonotoneX, curveStep, line } from 'd3-shape';
import { useMemo } from 'react';

import type { Stage } from '../../../../utils';
import { filterMap, formatNumber, groupBy, lookupBy } from '../../../../utils';

import type {
  ContinuousChartCurveType,
  ContinuousChartMarkType,
  StackedRangeDatum,
} from './continuous-chart-render';

import './continuous-chart-single-render.scss';

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

export interface ContinuousChartSingleRenderProps {
  data: StackedRangeDatum[];
  measureIndex: number;
  facets: string[] | undefined;
  facetColorizer: (facet: string) => string;
  markType: ContinuousChartMarkType;
  curveType?: ContinuousChartCurveType;
  timeScale: ScaleTime<number, number>;
  measureScale: ScaleLinear<number, number>;
  innerStage: Stage;
  yAxisPosition?: 'left' | 'right';
  showHorizontalGridlines?: boolean;
  selectedDatum?: StackedRangeDatum;
  selectionFinalized?: boolean;
  transform?: string;
  title?: string;
}

export const ContinuousChartSingleRender = function ContinuousChartSingleRender(
  props: ContinuousChartSingleRenderProps,
) {
  const {
    data,
    measureIndex,
    facets,
    facetColorizer,
    markType,
    curveType,
    timeScale,
    measureScale,
    innerStage,
    yAxisPosition,
    showHorizontalGridlines,
    selectedDatum,
    selectionFinalized,
    transform,
    title,
  } = props;

  const byFacet = useMemo(() => {
    if (markType === 'bar' || !data.length) return [];
    const isStacked = markType !== 'line';

    const effectiveFacets = facets || ['undefined'];
    const numFacets = effectiveFacets.length;

    // Fill in 0s and make sure that the facets are in the same order
    const fullTimeIntervals = groupBy(
      data,
      d => String(d.start),
      dataForStart => {
        if (numFacets === 1) return [dataForStart[0]];
        const facetToDatum = lookupBy(dataForStart, d => d.facet!);
        return effectiveFacets.map(
          (facet, facetIndex) =>
            facetToDatum[facet] || {
              ...dataForStart[0],
              facet,
              measures: [0],
              offset: isStacked
                ? Math.max(
                    0,
                    ...filterMap(effectiveFacets.slice(0, facetIndex), s => facetToDatum[s]).map(
                      d => d.offset + d.measures[measureIndex],
                    ),
                  )
                : 0,
            },
        );
      },
    );

    // Add nulls to mark gaps in data
    const seriesForFacet: Record<string, (StackedRangeDatum | null)[]> = {};
    for (const stack of effectiveFacets) {
      seriesForFacet[stack] = [];
    }

    let lastDatum: StackedRangeDatum | undefined;
    for (const fullTimeInterval of fullTimeIntervals) {
      const datum = fullTimeInterval[0];

      if (lastDatum && lastDatum.start !== datum.end) {
        for (const facet of effectiveFacets) {
          seriesForFacet[facet].push(null);
        }
      }

      for (let i = 0; i < numFacets; i++) {
        seriesForFacet[effectiveFacets[i]].push(fullTimeInterval[i]);
      }
      lastDatum = datum;
    }

    return Object.values(seriesForFacet);
  }, [markType, data, facets, measureIndex]);

  function startEndToXWidth({ start, end }: { start: number; end: number }) {
    const xStart = timeScale(start);
    const xEnd = timeScale(end);
    if (xEnd < 0 || innerStage.width < xStart) return;

    return {
      x: xStart,
      width: Math.max(xEnd - xStart - 1, 1),
    };
  }

  function datumToYHeight({ measures, offset }: StackedRangeDatum) {
    const y0 = measureScale(offset);
    const y = measureScale(measures[measureIndex] + offset);

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
      cy: measureScale(d.measures[measureIndex] + d.offset),
    };
  }

  const curve = getCurveFactory(curveType);

  const areaFn = area<StackedRangeDatum>()
    .curve(curve)
    .defined(Boolean)
    .x(d => timeScale((d.start + d.end) / 2))
    .y0(d => measureScale(d.offset))
    .y1(d => measureScale(d.measures[measureIndex] + d.offset)) as Area<StackedRangeDatum | null>;

  const lineFn = line<StackedRangeDatum>()
    .curve(curve)
    .defined(Boolean)
    .x(d => timeScale((d.start + d.end) / 2))
    .y(d => measureScale(d.measures[measureIndex] + d.offset)) as Line<StackedRangeDatum | null>;

  return (
    <g className="continuous-chart-single-render" transform={transform}>
      {title && (
        <text className="chart-title" x={5} y={15}>
          {title}
        </text>
      )}
      {showHorizontalGridlines && (
        <g className="h-gridline" transform="translate(0,0)">
          {filterMap(measureScale.ticks(3), (v, i) => {
            if (v === 0) return;
            const y = measureScale(v);
            return <line key={i} x1={0} y1={y} x2={innerStage.width} y2={y} />;
          })}
        </g>
      )}
      <g clipPath={`xywh(0px 0px ${innerStage.width}px ${innerStage.height}px) view-box`}>
        {markType === 'bar' &&
          filterMap(data, stackedRow => {
            const r = datumToRect(stackedRow);
            if (!r) return;
            return (
              <rect
                key={`${stackedRow.start}/${stackedRow.end}/${stackedRow.facet}`}
                className="mark-bar"
                {...r}
                style={
                  typeof stackedRow.facet !== 'undefined'
                    ? {
                        fill: facetColorizer(stackedRow.facet),
                      }
                    : undefined
                }
              />
            );
          })}
        {markType === 'bar' && selectedDatum && (
          <rect
            className={classNames('selected-bar', { finalized: selectionFinalized })}
            {...datumToRect(selectedDatum)}
          />
        )}
        {markType === 'area' &&
          byFacet.map(ds => {
            const facet = ds[0]!.facet;
            return (
              <path
                key={String(facet)}
                className="mark-area"
                d={areaFn(ds)!}
                style={
                  typeof facet !== 'undefined'
                    ? {
                        fill: facetColorizer(facet),
                      }
                    : undefined
                }
              />
            );
          })}
        {(markType === 'area' || markType === 'line') &&
          byFacet.map(ds => {
            const facet = ds[0]!.facet;
            return (
              <path
                key={String(facet)}
                className="mark-line"
                d={lineFn(ds)!}
                style={
                  typeof facet !== 'undefined'
                    ? {
                        stroke: facetColorizer(facet),
                      }
                    : undefined
                }
              />
            );
          })}
        {(markType === 'area' || markType === 'line') &&
          byFacet.flatMap(ds =>
            filterMap(ds, (d, i) => {
              if (!d || ds[i - 1] || ds[i + 1]) return; // Not a single point
              const x = timeScale((d.start + d.end) / 2);
              return (
                <line
                  key={`single_${i}_${d.facet}`}
                  className="single-point"
                  x1={x}
                  x2={x}
                  y1={measureScale(d.measures[measureIndex] + d.offset)}
                  y2={measureScale(d.offset)}
                  style={
                    typeof d.facet !== 'undefined'
                      ? {
                          stroke: facetColorizer(d.facet),
                        }
                      : undefined
                  }
                />
              );
            }),
          )}
        {(markType === 'area' || markType === 'line') && selectedDatum && (
          <circle
            className={classNames('selected-point', { finalized: selectionFinalized })}
            {...datumToCxCy(selectedDatum)}
            r={3}
            style={
              typeof selectedDatum.facet !== 'undefined'
                ? {
                    fill: facetColorizer(selectedDatum.facet),
                  }
                : undefined
            }
          />
        )}
      </g>
      {yAxisPosition === 'left' && (
        <g
          className="axis-y"
          ref={(node: any) =>
            select(node).call(
              axisLeft(measureScale)
                .ticks(3)
                .tickSizeOuter(0)
                .tickFormat(e => formatNumber(e.valueOf())),
            )
          }
        />
      )}
      {yAxisPosition === 'right' && (
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
  );
};
