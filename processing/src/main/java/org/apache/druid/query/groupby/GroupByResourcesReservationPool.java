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

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reserves the {@link GroupByQueryResources} for a given group by query and maps them to the query's resource ID.
 */
public class GroupByResourcesReservationPool
{
  /**
   * Map of query's resource id -> group by resources reserved for the query to execute
   */
  final ConcurrentHashMap<String, GroupByQueryResources> pool = new ConcurrentHashMap<>();

  /**
   * Buffer pool from where the merge buffers are picked and reserved
   */
  final BlockingPool<ByteBuffer> mergeBufferPool;

  /**
   * Group by query config of the server
   */
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

  /**
   * Reserves appropariate resources, and maps it to the queryResourceId (usually the query's resource id) in the internal map
   */
  public void reserve(String queryResourceId, GroupByQuery groupByQuery, boolean willMergeRunner)
  {
    if (queryResourceId == null) {
      throw DruidException.defensive("Query resource id must be populated");
    }
    pool.compute(queryResourceId, (id, existingResource) -> {
      if (existingResource != null) {
        throw DruidException.defensive("Resource with the given identifier [%s] is already present", id);
      }
      return GroupingEngine.prepareResource(groupByQuery, mergeBufferPool, willMergeRunner, groupByQueryConfig);
    });
  }

  /**
   * Fetches resources corresponding to the given resource id
   */
  @Nullable
  public GroupByQueryResources fetch(String queryResourceId)
  {
    return pool.get(queryResourceId);
  }

  /**
   * Removes the entry corresponding to the unique id from the map, and cleans up the resources.
   */
  public void clean(String queryResourceId)
  {
    GroupByQueryResources resources = pool.remove(queryResourceId);
    if (resources != null) {
      resources.close();
    }
  }
}
