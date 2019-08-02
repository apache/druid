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

import { filterMap } from './general';
import { DimensionMode, DimensionSpec, IngestionSpec, MetricSpec } from './ingestion-spec';
import { deepDelete, deepSet } from './object-change';
import { HeaderAndRows } from './sampler';

export function guessTypeFromSample(sample: any[]): string {
  const definedValues = sample.filter(v => v != null);
  if (
    definedValues.length &&
    definedValues.every(v => !isNaN(v) && (typeof v === 'number' || typeof v === 'string'))
  ) {
    if (definedValues.every(v => v % 1 === 0)) {
      return 'long';
    } else {
      return 'float';
    }
  } else {
    return 'string';
  }
}

export function getColumnTypeFromHeaderAndRows(
  headerAndRows: HeaderAndRows,
  column: string,
): string {
  return guessTypeFromSample(
    filterMap(headerAndRows.rows, (r: any) => (r.parsed ? r.parsed[column] : undefined)),
  );
}

export function getDimensionSpecs(
  headerAndRows: HeaderAndRows,
  hasRollup: boolean,
): (string | DimensionSpec)[] {
  return filterMap(headerAndRows.header, h => {
    if (h === '__time') return;
    const guessedType = getColumnTypeFromHeaderAndRows(headerAndRows, h);
    if (guessedType === 'string') return h;
    if (hasRollup) return;
    return {
      type: guessedType,
      name: h,
    };
  });
}

export function getMetricSecs(headerAndRows: HeaderAndRows): MetricSpec[] {
  return [{ name: 'count', type: 'count' }].concat(
    filterMap(headerAndRows.header, h => {
      if (h === '__time') return;
      const guessedType = getColumnTypeFromHeaderAndRows(headerAndRows, h);
      switch (guessedType) {
        case 'double':
          return { name: `sum_${h}`, type: 'doubleSum', fieldName: h };
        case 'long':
          return { name: `sum_${h}`, type: 'longSum', fieldName: h };
        default:
          return;
      }
    }),
  );
}

export function updateSchemaWithSample(
  spec: IngestionSpec,
  headerAndRows: HeaderAndRows,
  dimensionMode: DimensionMode,
  rollup: boolean,
): IngestionSpec {
  let newSpec = spec;

  if (dimensionMode === 'auto-detect') {
    newSpec = deepSet(newSpec, 'dataSchema.parser.parseSpec.dimensionsSpec.dimensions', []);
  } else {
    newSpec = deepDelete(newSpec, 'dataSchema.parser.parseSpec.dimensionsSpec.dimensionExclusions');

    const dimensions = getDimensionSpecs(headerAndRows, rollup);
    if (dimensions) {
      newSpec = deepSet(
        newSpec,
        'dataSchema.parser.parseSpec.dimensionsSpec.dimensions',
        dimensions,
      );
    }
  }

  if (rollup) {
    newSpec = deepSet(newSpec, 'dataSchema.granularitySpec.queryGranularity', 'HOUR');

    const metrics = getMetricSecs(headerAndRows);
    if (metrics) {
      newSpec = deepSet(newSpec, 'dataSchema.metricsSpec', metrics);
    }
  } else {
    newSpec = deepSet(newSpec, 'dataSchema.granularitySpec.queryGranularity', 'NONE');
    newSpec = deepDelete(newSpec, 'dataSchema.metricsSpec');
  }

  newSpec = deepSet(newSpec, 'dataSchema.granularitySpec.rollup', rollup);
  return newSpec;
}
