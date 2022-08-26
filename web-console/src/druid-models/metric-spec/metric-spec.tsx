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

import { Code } from '@blueprintjs/core';
import React from 'react';

import { ExternalLink, Field } from '../../components';
import { getLink } from '../../links';
import { filterMap, typeIs } from '../../utils';
import { SampleHeaderAndRows } from '../../utils/sampler';
import { guessColumnTypeFromHeaderAndRows } from '../ingestion-spec/ingestion-spec';

export interface MetricSpec {
  readonly type: string;
  readonly name?: string;
  readonly fieldName?: string;
  readonly maxStringBytes?: number;
  readonly filterNullValues?: boolean;
  readonly fieldNames?: string[];
  readonly fnAggregate?: string;
  readonly fnCombine?: string;
  readonly fnReset?: string;
  readonly fields?: string[];
  readonly byRow?: boolean;
  readonly round?: boolean;
  readonly isInputHyperUnique?: boolean;
  readonly filter?: any;
  readonly aggregator?: MetricSpec;
  readonly size?: number;
  readonly lgK?: number;
  readonly tgtHllType?: string;
  readonly k?: number;
}

export const METRIC_SPEC_FIELDS: Field<MetricSpec>[] = [
  {
    name: 'name',
    type: 'string',
    required: true,
    info: <>The metric name as it will appear in Druid.</>,
    placeholder: 'metric_name',
  },
  {
    name: 'type',
    type: 'string',
    required: true,
    suggestions: [
      'count',
      {
        group: 'sum',
        suggestions: ['longSum', 'doubleSum', 'floatSum'],
      },
      {
        group: 'min',
        suggestions: ['longMin', 'doubleMin', 'floatMin'],
      },
      {
        group: 'max',
        suggestions: ['longMax', 'doubleMax', 'floatMax'],
      },
      // Do not show first and last aggregators as they can not be used in ingestion specs and this definition is only used in the data loader.
      // Ref: https://druid.apache.org/docs/latest/querying/aggregations.html#first--last-aggregator
      // Should the first / last aggregators become usable at ingestion time, reverse the changes made in:
      // https://github.com/apache/druid/pull/10794
      'thetaSketch',
      {
        group: 'HLLSketch',
        suggestions: ['HLLSketchBuild', 'HLLSketchMerge'],
      },
      'quantilesDoublesSketch',
      'momentSketch',
      'fixedBucketsHistogram',
      'hyperUnique',
      'filtered',
    ],
    info: <>The aggregation function to apply.</>,
  },
  {
    name: 'fieldName',
    type: 'string',
    defined: typeIs(
      'longSum',
      'doubleSum',
      'floatSum',
      'longMin',
      'doubleMin',
      'floatMin',
      'longMax',
      'doubleMax',
      'floatMax',
      'thetaSketch',
      'HLLSketchBuild',
      'HLLSketchMerge',
      'quantilesDoublesSketch',
      'momentSketch',
      'fixedBucketsHistogram',
      'hyperUnique',
    ),
    required: true,
    placeholder: 'column_name',
    info: <>The column name for the aggregator to operate on.</>,
  },
  {
    name: 'maxStringBytes',
    type: 'number',
    defaultValue: 1024,
    defined: typeIs('stringFirst', 'stringLast'),
  },
  {
    name: 'filterNullValues',
    type: 'boolean',
    defaultValue: false,
    defined: typeIs('stringFirst', 'stringLast'),
  },
  // filtered
  {
    name: 'filter',
    type: 'json',
    defined: typeIs('filtered'),
    required: true,
  },
  {
    name: 'aggregator',
    type: 'json',
    defined: typeIs('filtered'),
    required: true,
  },
  // thetaSketch
  {
    name: 'size',
    type: 'number',
    defined: typeIs('thetaSketch'),
    defaultValue: 16384,
    info: (
      <>
        <p>
          Must be a power of 2. Internally, size refers to the maximum number of entries sketch
          object will retain. Higher size means higher accuracy but more space to store sketches.
          Note that after you index with a particular size, druid will persist sketch in segments
          and you will use size greater or equal to that at query time.
        </p>
        <p>
          See the{' '}
          <ExternalLink href="https://datasketches.apache.org/docs/Theta/ThetaSize.html">
            DataSketches site
          </ExternalLink>{' '}
          for details.
        </p>
        <p>In general, We recommend just sticking to default size.</p>
      </>
    ),
  },
  {
    name: 'isInputThetaSketch',
    type: 'boolean',
    defined: typeIs('thetaSketch'),
    defaultValue: false,
    info: (
      <>
        This should only be used at indexing time if your input data contains theta sketch objects.
        This would be the case if you use datasketches library outside of Druid, say with Pig/Hive,
        to produce the data that you are ingesting into Druid
      </>
    ),
  },
  // HLLSketchBuild & HLLSketchMerge
  {
    name: 'lgK',
    type: 'number',
    defined: typeIs('HLLSketchBuild', 'HLLSketchMerge'),
    defaultValue: 12,
    info: (
      <>
        <p>
          log2 of K that is the number of buckets in the sketch, parameter that controls the size
          and the accuracy.
        </p>
        <p>Must be between 4 to 21 inclusively.</p>
      </>
    ),
  },
  {
    name: 'tgtHllType',
    type: 'string',
    defined: typeIs('HLLSketchBuild', 'HLLSketchMerge'),
    defaultValue: 'HLL_4',
    suggestions: ['HLL_4', 'HLL_6', 'HLL_8'],
    info: (
      <>
        The type of the target HLL sketch. Must be <Code>HLL_4</Code>, <Code>HLL_6</Code>, or{' '}
        <Code>HLL_8</Code>.
      </>
    ),
  },
  // quantilesDoublesSketch
  {
    name: 'k',
    type: 'number',
    defined: typeIs('quantilesDoublesSketch'),
    defaultValue: 128,
    info: (
      <>
        <p>
          Parameter that determines the accuracy and size of the sketch. Higher k means higher
          accuracy but more space to store sketches.
        </p>
        <p>
          Must be a power of 2 from 2 to 32768. See the{' '}
          <ExternalLink href="https://datasketches.apache.org/docs/Quantiles/QuantilesAccuracy.html">
            Quantiles Accuracy
          </ExternalLink>{' '}
          for details.
        </p>
      </>
    ),
  },
  // momentSketch
  {
    name: 'k',
    type: 'number',
    defined: typeIs('momentSketch'),
    required: true,
    info: (
      <>
        Parameter that determines the accuracy and size of the sketch. Higher k means higher
        accuracy but more space to store sketches. Usable range is generally [3,15]
      </>
    ),
  },
  {
    name: 'compress',
    type: 'boolean',
    defined: typeIs('momentSketch'),
    defaultValue: true,
    info: (
      <>
        Flag for whether the aggregator compresses numeric values using arcsinh. Can improve
        robustness to skewed and long-tailed distributions, but reduces accuracy slightly on more
        uniform distributions.
      </>
    ),
  },
  // fixedBucketsHistogram
  {
    name: 'lowerLimit',
    type: 'number',
    defined: typeIs('fixedBucketsHistogram'),
    required: true,
    info: <>Lower limit of the histogram.</>,
  },
  {
    name: 'upperLimit',
    type: 'number',
    defined: typeIs('fixedBucketsHistogram'),
    required: true,
    info: <>Upper limit of the histogram.</>,
  },
  {
    name: 'numBuckets',
    type: 'number',
    defined: typeIs('fixedBucketsHistogram'),
    defaultValue: 10,
    required: true,
    info: (
      <>
        Number of buckets for the histogram. The range <Code>[lowerLimit, upperLimit]</Code> will be
        divided into <Code>numBuckets</Code> intervals of equal size.
      </>
    ),
  },
  {
    name: 'outlierHandlingMode',
    type: 'string',
    defined: typeIs('fixedBucketsHistogram'),
    required: true,
    suggestions: ['ignore', 'overflow', 'clip'],
    info: (
      <>
        <p>
          Specifies how values outside of <Code>[lowerLimit, upperLimit]</Code> will be handled.
        </p>
        <p>
          Supported modes are <Code>ignore</Code>, <Code>overflow</Code>, and <Code>clip</Code>. See
          <ExternalLink
            href={`${getLink(
              'DOCS',
            )}/development/extensions-core/approximate-histograms.html#outlier-handling-modes`}
          >
            outlier handling modes
          </ExternalLink>{' '}
          for more details.
        </p>
      </>
    ),
  },
  // hyperUnique
  {
    name: 'isInputHyperUnique',
    type: 'boolean',
    defined: typeIs('hyperUnique'),
    defaultValue: false,
    info: (
      <>
        This can be set to true to index precomputed HLL (Base64 encoded output from druid-hll is
        expected).
      </>
    ),
  },
];

export function getMetricSpecName(metricSpec: MetricSpec): string {
  return (
    metricSpec.name || (metricSpec.aggregator ? getMetricSpecName(metricSpec.aggregator) : '?')
  );
}

export function getMetricSpecSingleFieldName(metricSpec: MetricSpec): string | undefined {
  return (
    metricSpec.fieldName ||
    (metricSpec.aggregator ? getMetricSpecSingleFieldName(metricSpec.aggregator) : undefined)
  );
}

export function getMetricSpecOutputType(metricSpec: MetricSpec): string | undefined {
  if (metricSpec.aggregator) return getMetricSpecOutputType(metricSpec.aggregator);
  const m = /^(long|float|double|string)/.exec(String(metricSpec.type));
  if (!m) return;
  return m[1];
}

export function getMetricSpecs(
  headerAndRows: SampleHeaderAndRows,
  typeHints: Record<string, string>,
  guessNumericStringsAsNumbers: boolean,
): MetricSpec[] {
  return [{ name: 'count', type: 'count' }].concat(
    filterMap(headerAndRows.header, h => {
      if (h === '__time') return;
      const type =
        typeHints[h] ||
        guessColumnTypeFromHeaderAndRows(headerAndRows, h, guessNumericStringsAsNumbers);
      switch (type) {
        case 'double':
          return { name: `sum_${h}`, type: 'doubleSum', fieldName: h };
        case 'float':
          return { name: `sum_${h}`, type: 'floatSum', fieldName: h };
        case 'long':
          return { name: `sum_${h}`, type: 'longSum', fieldName: h };
        default:
          return;
      }
    }),
  );
}
