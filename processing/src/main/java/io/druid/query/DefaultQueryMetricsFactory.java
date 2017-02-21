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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.query.groupby.DefaultGroupByQueryMetrics;
import io.druid.query.groupby.GroupByQueryMetrics;
import io.druid.query.timeseries.DefaultTimeseriesQueryMetrics;
import io.druid.query.timeseries.TimeseriesQueryMetrics;
import io.druid.query.topn.DefaultTopNQueryMetrics;
import io.druid.query.topn.TopNQueryMetrics;

public class DefaultQueryMetricsFactory implements QueryMetricsFactory
{
  private static final DefaultQueryMetricsFactory INSTANCE = new DefaultQueryMetricsFactory(new ObjectMapper());

  public static DefaultQueryMetricsFactory instance()
  {
    return INSTANCE;
  }

  private final ObjectMapper jsonMapper;

  @Inject
  public DefaultQueryMetricsFactory(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(Query<?> query)
  {
    DefaultQueryMetrics<Query<?>> queryMetrics = new DefaultQueryMetrics<>(jsonMapper);
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public TopNQueryMetrics makeTopNQueryMetrics()
  {
    return new DefaultTopNQueryMetrics(jsonMapper);
  }

  @Override
  public GroupByQueryMetrics makeGroupByQueryMetrics()
  {
    return new DefaultGroupByQueryMetrics(jsonMapper);
  }

  @Override
  public TimeseriesQueryMetrics makeTimeseriesQueryMetrics()
  {
    return new DefaultTimeseriesQueryMetrics(jsonMapper);
  }
}
