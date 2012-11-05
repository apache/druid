/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid;

import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.AllGranularity;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesBinaryFn
    implements BinaryFn<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>
{
  private final QueryGranularity gran;
  private final List<AggregatorFactory> aggregations;
  private final List<PostAggregator> postAggregations;

  public TimeseriesBinaryFn(
      QueryGranularity granularity,
      List<AggregatorFactory> aggregations,
      List<PostAggregator> postAggregations
  )
  {
    this.gran = granularity;
    this.aggregations = aggregations;
    this.postAggregations = postAggregations;
  }

  @Override
  public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> arg1, Result<TimeseriesResultValue> arg2)
  {
    if (arg1 == null) {
      return arg2;
    }

    if (arg2 == null) {
      return arg1;
    }

    TimeseriesResultValue arg1Val = arg1.getValue();
    TimeseriesResultValue arg2Val = arg2.getValue();

    Map<String, Object> retVal = new LinkedHashMap<String, Object>();

    for (AggregatorFactory factory : aggregations) {
      final String metricName = factory.getName();
      retVal.put(metricName, factory.combine(arg1Val.getMetric(metricName), arg2Val.getMetric(metricName)));
    }

    for (PostAggregator pf : postAggregations) {
      final String metricName = pf.getName();
      retVal.put(metricName, pf.compute(retVal));
    }

    return (gran instanceof AllGranularity) ?
           new Result<TimeseriesResultValue>(
               arg1.getTimestamp(),
               new TimeseriesResultValue(retVal)
           ) :
           new Result<TimeseriesResultValue>(
               gran.toDateTime(gran.truncate(arg1.getTimestamp().getMillis())),
               new TimeseriesResultValue(retVal)
           );
  }

}
