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

package org.apache.druid.query;

import com.google.common.collect.Maps;
import com.google.inject.Inject;
import java.util.IdentityHashMap;
import java.util.Map;

public class DefaultQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
{
  private final Map<Class<? extends Query>, QueryRunnerFactory> factories;
  private final Map<Class<? extends Query>, QueryToolChest> toolchests;

  public static DefaultQueryRunnerFactoryConglomerate buildFromQueryRunnerFactories(
      Map<Class<? extends Query>, QueryRunnerFactory> factories)
  {
    return new DefaultQueryRunnerFactoryConglomerate(factories, Maps.transformValues(factories, f -> f.getToolchest()));
  }

  @Inject
  public DefaultQueryRunnerFactoryConglomerate(Map<Class<? extends Query>, QueryRunnerFactory> factories,
      Map<Class<? extends Query>, QueryToolChest> toolchests)
  {
    this.factories = new IdentityHashMap<>(factories);
    this.toolchests = new IdentityHashMap<>(toolchests);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
  {
    return factories.get(query.getClass());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    return toolchests.get(query.getClass());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryExecutor<T> getQueryExecutor(QueryType query)
  {
    QueryToolChest<T, QueryType> toolchest = getToolChest(query);
    if (toolchest instanceof QueryExecutor) {
      return (QueryExecutor<T>) toolchest;
    }
    return null;
  }
}
