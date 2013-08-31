/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query.timeseries;

import io.druid.query.Result;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class TimeseriesResultBuilder
{
  private final DateTime timestamp;

  private final Map<String, Object> metricValues = new HashMap<String, Object>();

  public TimeseriesResultBuilder(
      DateTime timestamp
  )
  {
    this.timestamp = timestamp;
  }

  public TimeseriesResultBuilder addMetric(Aggregator aggregator)
  {
    metricValues.put(aggregator.getName(), aggregator.get());
    return this;
  }

  public TimeseriesResultBuilder addMetric(PostAggregator postAggregator)
  {
    metricValues.put(postAggregator.getName(), postAggregator.compute(metricValues));
    return this;
  }

  public Result<TimeseriesResultValue> build()
  {
    return new Result<TimeseriesResultValue>(
        timestamp,
        new TimeseriesResultValue(metricValues)
    );
  }
}