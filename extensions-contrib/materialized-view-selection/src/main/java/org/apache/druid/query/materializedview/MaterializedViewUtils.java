/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.materializedview;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class MaterializedViewUtils 
{
  /**
   * extract all dimensions in query.
   * only support TopNQuery/TimeseriesQuery/GroupByQuery
   * 
   * @param query
   * @return dimensions set in query
   */
  public static Set<String> getRequiredFields(Query query)
  {
    Set<String> dimensions = new HashSet<>();
    Set<String> dimsInFilter = null == query.getFilter() ? new HashSet<String>() : query.getFilter().getRequiredColumns();
    dimensions.addAll(dimsInFilter);

    if (query instanceof TopNQuery) {
      TopNQuery q = (TopNQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
      dimensions.add(q.getDimensionSpec().getDimension());
    } else if (query instanceof TimeseriesQuery) {
      TimeseriesQuery q = (TimeseriesQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
    } else if (query instanceof GroupByQuery) {
      GroupByQuery q = (GroupByQuery) query;
      dimensions.addAll(extractFieldsFromAggregations(q.getAggregatorSpecs()));
      for (DimensionSpec spec : q.getDimensions()) {
        String dim = spec.getDimension();
        dimensions.add(dim);
      }
    } else {
      throw new UnsupportedOperationException("Method getRequeiredFields only support TopNQuery/TimeseriesQuery/GroupByQuery");
    }
    return dimensions;
  }

  private static Set<String> extractFieldsFromAggregations(List<AggregatorFactory> aggs) 
  {
    Set<String> ret = new HashSet<>();
    for (AggregatorFactory agg : aggs) {
      if (agg instanceof FilteredAggregatorFactory) {
        FilteredAggregatorFactory fagg = (FilteredAggregatorFactory) agg;
        ret.addAll(fagg.getFilter().getRequiredColumns());
      }
      ret.addAll(agg.requiredFields());
    }
    return ret;
  }

  /**
   * calculate the intervals which are covered by interval2, but not covered by interval1.
   * result intervals = interval2 - interval1 ∩ interval2
   * e.g. 
   * a list of interval2: ["2018-04-01T00:00:00.000Z/2018-04-02T00:00:00.000Z",
   *                       "2018-04-03T00:00:00.000Z/2018-04-10T00:00:00.000Z"]
   * a list of interval1: ["2018-04-04T00:00:00.000Z/2018-04-06T00:00:00.000Z"]
   * the result list of intervals: ["2018-04-01T00:00:00.000Z/2018-04-02T00:00:00.000Z",
   *                                "2018-04-03T00:00:00.000Z/2018-04-04T00:00:00.000Z",
   *                                "2018-04-06T00:00:00.000Z/2018-04-10T00:00:00.000Z"]
   * If interval2 is empty, then return an empty list of interval.                           
   * @param interval2 list of intervals
   * @param interval1 list of intervals
   * @return list of intervals are covered by interval2, but not covered by interval1.
   */
  public static List<Interval> minus(List<Interval> interval2, List<Interval> interval1)
  {
    if (interval1.isEmpty() || interval2.isEmpty()) {
      return interval1;
    }
    Iterator<Interval> it1 = JodaUtils.condenseIntervals(interval1).iterator();
    Iterator<Interval> it2 = JodaUtils.condenseIntervals(interval2).iterator();
    List<Interval> remaining = new ArrayList<>();
    Interval currInterval1 = it1.next();
    Interval currInterval2 = it2.next();
    long start1 = currInterval1.getStartMillis();
    long end1 = currInterval1.getEndMillis();
    long start2 = currInterval2.getStartMillis();
    long end2 = currInterval2.getEndMillis();
    while (true) {
      if (start2 < start1 && end2 <= start1) {
        remaining.add(Intervals.utc(start2, end2));
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 < start1 && end2 > start1 && end2 < end1) {
        remaining.add(Intervals.utc(start2, start1));
        start1 = end2;
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 < start1 && end2 == end1) {
        remaining.add(Intervals.utc(start2, start1));
        if (it2.hasNext() && it1.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 < start1 && end2 > end1) {
        remaining.add(Intervals.utc(start2, start1));
        start2 = end1;
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(end1, end2));
          break;
        }
      }
      if (start2 == start1 && end2 >= start1 && end2 < end1) {
        start1 = end2;
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 == start1 && end2 > end1) {
        start2 = end1;
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(end1, end2));
          break;
        }
      }
      if (start2 > start1 && start2 < end1 && end2 < end1) {
        start1 = end2;
        if (it2.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 > start1 && start2 < end1 && end2 > end1) {
        start2 = end1;
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(end1, end2));
          break;
        }
      }
      if (start2 >= start1 && start2 <= end1 && end2 == end1) {
        if (it2.hasNext() && it1.hasNext()) {
          currInterval2 = it2.next();
          start2 = currInterval2.getStartMillis();
          end2 = currInterval2.getEndMillis();
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          break;
        }
      }
      if (start2 >= end1 && end2 > end1) {
        if (it1.hasNext()) {
          currInterval1 = it1.next();
          start1 = currInterval1.getStartMillis();
          end1 = currInterval1.getEndMillis();
        } else {
          remaining.add(Intervals.utc(start2, end2));
          break;
        }
      }
    }

    while (it2.hasNext()) {
      remaining.add(Intervals.of(it2.next().toString()));
    }
    return remaining;
  }
}
