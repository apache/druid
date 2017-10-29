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

package io.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.druid.guice.LazySingleton;
import io.druid.guice.annotations.Json;
import io.druid.jackson.DefaultObjectMapper;

@LazySingleton
public class DefaultGroupByQueryMetricsFactory implements GroupByQueryMetricsFactory
{
  private static final GroupByQueryMetricsFactory INSTANCE =
      new DefaultGroupByQueryMetricsFactory(new DefaultObjectMapper());

  /**
   * Should be used only in tests, directly or indirectly (via {@link
   * GroupByQueryQueryToolChest#GroupByQueryQueryToolChest(io.druid.query.groupby.strategy.GroupByStrategySelector,
   * io.druid.query.IntervalChunkingQueryRunnerDecorator)}).
   */
  @VisibleForTesting
  public static GroupByQueryMetricsFactory instance()
  {
    return INSTANCE;
  }

  private final ObjectMapper jsonMapper;

  @Inject
  public DefaultGroupByQueryMetricsFactory(@Json ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public GroupByQueryMetrics makeMetrics()
  {
    return new DefaultGroupByQueryMetrics(jsonMapper);
  }
}
