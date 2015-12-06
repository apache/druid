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
