/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.materializedview;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.JodaUtils;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.IntervalDimFilter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import org.joda.time.Interval;

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
    Set<String> dimsInFilter = getDimensionsInFilter(query.getFilter());
    if (dimsInFilter != null) {
      dimensions.addAll(dimsInFilter);
    }
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
    }
    return dimensions;
  }

  private static Set<String> extractFieldsFromAggregations(List<AggregatorFactory> aggs) 
  {
    Set<String> ret = new HashSet<>();
    for (AggregatorFactory agg : aggs) {
      if (agg instanceof FilteredAggregatorFactory) {
        FilteredAggregatorFactory fagg = (FilteredAggregatorFactory) agg;
        ret.addAll(getDimensionsInFilter(fagg.getFilter()));
      }
      ret.addAll(agg.requiredFields());
    }
    return ret;
  }

  private static Set<String> getDimensionsInFilter(DimFilter dimFilter)
  {
    if (dimFilter instanceof AndDimFilter) {
      AndDimFilter d = (AndDimFilter) dimFilter;
      Set<String> ret = new HashSet<>();
      for (DimFilter filter : d.getFields()) {
        ret.addAll(getDimensionsInFilter(filter));
      }
      return ret;
    } else if (dimFilter instanceof OrDimFilter) {
      OrDimFilter d = (OrDimFilter) dimFilter;
      Set<String> ret = new HashSet<>();
      for (DimFilter filter : d.getFields()) {
        ret.addAll(getDimensionsInFilter(filter));
      }
      return ret;
    } else if (dimFilter instanceof NotDimFilter) {
      NotDimFilter d = (NotDimFilter) dimFilter;
      return getDimensionsInFilter(d.getField());
    } else if (dimFilter instanceof BoundDimFilter) {
      BoundDimFilter d = (BoundDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else if (dimFilter instanceof InDimFilter) {
      InDimFilter d = (InDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else if (dimFilter instanceof IntervalDimFilter) {
      IntervalDimFilter d = (IntervalDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else if (dimFilter instanceof LikeDimFilter) {
      LikeDimFilter d = (LikeDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else if (dimFilter instanceof RegexDimFilter) {
      RegexDimFilter d = (RegexDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else if (dimFilter instanceof SearchQueryDimFilter) {
      SearchQueryDimFilter d = (SearchQueryDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else if (dimFilter instanceof SelectorDimFilter) {
      SelectorDimFilter d = (SelectorDimFilter) dimFilter;
      return Sets.newHashSet(d.getDimension());
    } else {
      return null;
    }
  }

  /**
   * interval2 - interval1
   * 
   * @param interval2
   * @param interval1
   * @return results of interval2-interval1
   */
  public static List<Interval> minus(List<Interval> interval2, List<Interval> interval1)
  {
    if (interval1.isEmpty() || interval2.isEmpty()) {
      return interval1;
    }
    Iterator<Interval> it1 = JodaUtils.condenseIntervals(interval1).iterator();
    Iterator<Interval> it2 = JodaUtils.condenseIntervals(interval2).iterator();
    List<Interval> remaining = Lists.newArrayList();
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
