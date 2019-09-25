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

import org.apache.druid.java.util.common.logger.Logger;

/**
 */
@Deprecated
public class ReflectionQueryToolChestWarehouse implements QueryToolChestWarehouse
{
  private static final Logger log = new Logger(ReflectionQueryToolChestWarehouse.class);

  private final ClassValue<QueryToolChest<?, ?>> toolChests = new ClassValue<QueryToolChest<?, ?>>()
  {
    @Override
    protected QueryToolChest<?, ?> computeValue(Class<?> type)
    {
      try {
        final Class<?> queryToolChestClass = Class.forName(type.getName() + "QueryToolChest");
        return (QueryToolChest<?, ?>) queryToolChestClass.newInstance();
      }
      catch (Exception e) {
        log.warn(e, "Unable to load interface[QueryToolChest] for input class[%s]", type);
        throw new RuntimeException(e);
      }
    }
  };

  @Override
  @SuppressWarnings("unchecked")
  public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
  {
    return (QueryToolChest<T, QueryType>) toolChests.get(query.getClass());
  }
}
