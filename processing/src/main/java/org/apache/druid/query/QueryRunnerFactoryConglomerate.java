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

import org.apache.druid.error.DruidException;

/**
 */
public interface QueryRunnerFactoryConglomerate extends QueryToolChestWarehouse
{
  <T, QueryType extends Query<T>> QueryRunnerFactory<T, QueryType> findFactory(QueryType query);

  @Override
  default <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    QueryRunnerFactory<T, QueryType> factory = findFactory(query);
    if (factory == null) {
      throw DruidException
          .defensive("QueryRunnerFactory for QueryType [%s] is not registered!", query.getClass().getName());
    }
    return factory.getToolchest();
  }
  // FIXME: add method here

}
