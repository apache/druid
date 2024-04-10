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
import org.apache.druid.query.QueryResourceId;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reserves the {@link GroupByQueryResources} for a given group by query and maps them to the query's resource ID.
 * The merge buffers can be required for the group by query in a couple of places:
 * 1. {@link GroupByQueryQueryToolChest}
 * 2. {@link GroupByResourcesReservationPool}
 * However, acquiring them separately can lead to deadlocks when multiple queries are fired. Therefore, instead of
 * acquiring them separately, we acquire them once during the query execution, in {@link GroupByQueryQueryToolChest} and
 * use those resources till the query is active.
 * <p>
 * ALLOCATION
 * The merge buffers are allocated and associated with a given resource id in this pool. Multiple attempts to insert the same resource id will fail,
 * therefore we know that there will only be resources allocated only once, as long as the query id doesn't change during the execution of the query.
 * The pool is cleaned once close() is called on the reserved resources, and the mapping is removed, thus ensuring that the mapping doesn't keep growing
 * during the execution of the queries.
 * The call to allocate the merge buffers in the pool is done by mergeResults, and it allocates the resources required for its execution as well as the
 * execution of the GroupByMergingQueryRunner if willMergeRunners=true. The GroupByMergingQueryRunner doesn't allocate any resources, it assumes that the resources
 * have been preallocated, and just takes them from the pool.
 * Once the required merge buffers are allocated from the pool, they cannot be used by the other queries till the close() method is called on the GroupByQueryResource.
 * This is usually done with a call to the GroupByResourcesReservationPool#clean() which does this and also cleans up the mapping.
 * While the GroupByQueryResource is unclosed, the merge buffers can be taken and given back to it as needed during the execution of the query. As such, the resources are not
 * released back to the global pool, and only given back to signify that the work of that execution unit is complete, and it can be reused (or closed safely). Closing the GroupByQueryResources
 * when all the merge buffers are not acquired back from the individual execution units log a warning, but doesn't throw.
 * The resources get freed up, and if the execution unit was actually using the resources for something, it can error out.
 * <p>
 * ASSUMPTIONS
 * There's an attempt to link various places where the merge buffers are acquired ({@link org.apache.druid.query.QueryToolChest#mergeResults})
 * and merge buffers are utilized ({@link org.apache.druid.query.QueryToolChest#mergeResults} and {@link GroupByQueryRunnerFactory.GroupByQueryRunner#mergeRunners}).
 * However, Druid's code doesn't provide any explicit contract between the arguments of these methods, and input to {@code mergeResults} can be any runner,
 * and it should function the same. While this provides flexibility and reusability to the methods, this also necessitates that there are some assumptions
 * that the code makes implicitly, to know what type of runner is passed to mergeResults - so that the mergeResults can allocate
 * the merge buffers required for the runner appropriately.
 * <p>
 * 1. For a given query, and a given server, only a single top-level mergeResults call will be made, that will collect the results from the various runners.
 * The code will break down if there are multiple, nested mergeResults calls made (unnested calls are fine, though they don't happen)
 * 2. There can be multiple mergeRunners, because GroupByMergingQueryRunner only needs the merge buffers for the top-level query runner,
 * nested ones execute via an unoptimized way.
 * 3. There's some knowledge to the mergeResults that the query runner passed to it is the one created by the corresponding toolchest's
 * mergeRunners (which is the typical use case). This is encoded in the argument {@code willMergeRunner}, and is to be set by the callers.
 * The only production use case where this isn't true is when the broker is merging the results gathered from the historical)
 * <p>
 * TESTING
 * Unit tests mimic the broker-historical interaction in many places, which can lead to the code not working as intended because the assumptions don't hold.
 * In many test cases, there are two nested mergeResults calls, the outer call mimics what the broker does, while the inner one mimics what the historical does,
 * and the assumption (1) fails. Therefore, the testing code should assign a unique resource id b/w each mergeResults call, and also make sure that the top level mergeResults
 * would have willMergeRunner = false, since it's being called on top of a mergeResults's runner, while the inner one would have willMergeRunner = true because its being
 * called on actual runners (as it happens in the brokers, and the historicals)
 */
public class GroupByResourcesReservationPool
{
  /**
   * Map of query's resource id -> group by resources reserved for the query to execute
   */
  final ConcurrentHashMap<QueryResourceId, GroupByQueryResources> pool = new ConcurrentHashMap<>();

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
   * Reserves appropriate resources, and maps it to the queryResourceId (usually the query's resource id) in the internal map
   */
  public void reserve(QueryResourceId queryResourceId, GroupByQuery groupByQuery, boolean willMergeRunner)
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
  public GroupByQueryResources fetch(QueryResourceId queryResourceId)
  {
    return pool.get(queryResourceId);
  }

  /**
   * Removes the entry corresponding to the unique id from the map, and cleans up the resources.
   */
  public void clean(QueryResourceId queryResourceId)
  {
    GroupByQueryResources resources = pool.remove(queryResourceId);
    if (resources != null) {
      resources.close();
    }
  }
}
