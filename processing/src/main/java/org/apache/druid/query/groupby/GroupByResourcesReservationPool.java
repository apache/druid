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

package org.apache.druid.query.groupby;

import org.apache.druid.collections.BlockingPool;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Merging;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class GroupByResourcesReservationPool
{
  final ConcurrentHashMap<String, GroupByQueryResources> pool = new ConcurrentHashMap<>();
  final BlockingPool<ByteBuffer> mergeBufferPool;
  final GroupByQueryConfig groupByQueryConfig;

  @Inject
  public GroupByResourcesReservationPool(
     @Merging BlockingPool<ByteBuffer> mergeBufferPool,
     GroupByQueryConfig groupByQueryConfig
  )
  {
    this.mergeBufferPool = mergeBufferPool;
    this.groupByQueryConfig = groupByQueryConfig;
  }

  public void reserve(String uniqueId, GroupByQuery groupByQuery, boolean willMergeRunner)
  {
    pool.compute(uniqueId, (id, existingResource) -> {
      if (existingResource != null) {
        throw DruidException.defensive("Resource with the given identifier [%s] is already present", id);
      }
      return GroupingEngine.prepareResource(groupByQuery, mergeBufferPool, willMergeRunner, groupByQueryConfig);
    });
  }

  public GroupByQueryResources fetch(String uniqueId)
  {
    return pool.get(uniqueId);
  }

  public void clean(String uniqueId)
  {
    pool.remove(uniqueId);
  }
}
