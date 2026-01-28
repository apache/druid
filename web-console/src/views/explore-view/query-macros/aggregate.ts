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

import { C, SqlFunction, SqlQuery } from 'druid-query-toolkit';

import { filterMap, uniq } from '../../../utils';
import { Measure } from '../models';
import { KNOWN_AGGREGATIONS } from '../utils';

export function rewriteAggregate(query: SqlQuery, measures: Measure[]): SqlQuery {
  const usedMeasures = new Map<string, boolean>();
  const queriesToRewrite: SqlQuery[] = [];
  const newQuery = query.walk(ex => {
    if (ex instanceof SqlFunction && ex.getEffectiveFunctionName() === Measure.AGGREGATE) {
      if (ex.numArgs() !== 1)
        throw new Error(`${Measure.AGGREGATE} function must have exactly 1 argument`);

      const measureName = ex.getArgAsString(0);
      if (!measureName) throw new Error(`${Measure.AGGREGATE} argument must be a measure name`);

      const measure = measures.find(({ name }) => name === measureName);
      if (!measure) throw new Error(`${Measure.AGGREGATE} of unknown measure '${measureName}'`);

      usedMeasures.set(measureName, true);

      let measureExpression = measure.expression;
      const filter = ex.getWhereExpression();
      if (filter) {
        measureExpression = measureExpression.addFilterToAggregations(filter, KNOWN_AGGREGATIONS);
      }

      return measureExpression;
    }

    // If we encounter a (the) query with the measure definitions, and we have used those measures then expand out all the columns within them
    if (ex instanceof SqlQuery) {
      const queryMeasures = Measure.extractQueryMeasures(ex);
      if (queryMeasures.length) {
        queriesToRewrite.push(ex);
      }
    }

    return ex;
  }) as SqlQuery;

  if (!queriesToRewrite.length) return newQuery;

  return newQuery.walk(subQuery => {
    if (subQuery instanceof SqlQuery && queriesToRewrite.includes(subQuery)) {
      return subQuery.applyForEach(
        uniq(
          filterMap(measures, queryMeasure =>
            usedMeasures.get(queryMeasure.name) ? queryMeasure.expression : undefined,
          ).flatMap(ex => ex.getUsedColumnNames()),
        ).filter(columnName => subQuery.getSelectIndexForOutputColumn(columnName) === -1),
        (q, columnName) => q.addSelect(C(columnName)),
      );
    }

    return subQuery;
  }) as SqlQuery;
}
