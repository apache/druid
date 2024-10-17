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

import com.google.inject.Inject;
import org.apache.druid.error.DruidException;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

public class DefaultQueryRunnerFactoryConglomerate implements QueryRunnerFactoryConglomerate
{
  private final Map<Class<? extends Query>, QueryRunnerFactory> factories;

  public DefaultQueryRunnerFactoryConglomerate(Map<Class<? extends Query>, QueryRunnerFactory> factories)
  {
    this(factories, Collections.emptyMap());
  }

  @Inject
  public DefaultQueryRunnerFactoryConglomerate(Map<Class<? extends Query>, QueryRunnerFactory> factories,
      Map<Class<? extends Query>, QueryToolChest> toolchests)
  {
    this.factories = new IdentityHashMap<>(factories);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query)
  {
    return factories.get(query.getClass());
  }

  @Override
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    QueryRunnerFactory<T, QueryType> factory = findFactory(query);
    if (factory == null) {
      throw DruidException
          .defensive("QueryRunnerFactory for QueryType [%s] is not registered!", query.getClass().getName());
    }
    return factory.getToolchest();
  }

  public <T, QueryType extends Query<T>> QueryExecutor<QueryType> getQueryExecutor(QueryType query)
  {
    QueryRunnerFactory<T, QueryType> factory = findFactory(query);
    if (factory == null) {
      throw DruidException
          .defensive("QueryRunnerFactory for QueryType [%s] is not registered!", query.getClass().getName());
    }
    QueryToolChest<T, QueryType> toolchest = factory.getToolchest();
    if (toolchest instanceof QueryExecutor) {
      return (QueryExecutor<QueryType>) toolchest;
    }
    return null;
  }

}
