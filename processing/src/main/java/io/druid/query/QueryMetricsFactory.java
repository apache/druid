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

package io.druid.query;

import io.druid.query.groupby.GroupByQueryMetrics;
import io.druid.query.timeseries.TimeseriesQueryMetrics;
import io.druid.query.topn.TopNQueryMetrics;

/**
 * This factory is used for DI of custom {@link QueryMetrics} implementations.
 */
public interface QueryMetricsFactory
{
  /**
   * Creates a {@link QueryMetrics} for query, which doesn't have predefined QueryMetrics subclass (i. e. not
   * a topN, groupBy or timeseries query). This method must call {@link QueryMetrics#query(Query)} with the given query
   * on the created QueryMetrics object before returning.
   */
  QueryMetrics<Query<?>> makeMetrics(Query<?> query);

  TopNQueryMetrics makeTopNQueryMetrics();

  GroupByQueryMetrics makeGroupByQueryMetrics();

  TimeseriesQueryMetrics makeTimeseriesQueryMetrics();
}
