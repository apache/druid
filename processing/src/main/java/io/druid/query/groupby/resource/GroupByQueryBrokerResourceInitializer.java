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

package io.druid.query.groupby.resource;

import com.google.inject.Inject;
import io.druid.collections.BlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.guice.annotations.Merging;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.QueryDataSource;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.strategy.GroupByStrategySelector;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is responsible for initializing a {@link GroupByQueryBrokerResource} before executin a group-by query.
 */
public class GroupByQueryBrokerResourceInitializer
{
  private final GroupByStrategySelector strategySelector;
  private final BlockingPool<ByteBuffer> mergeBufferPool;

  @Inject
  public GroupByQueryBrokerResourceInitializer(
      GroupByStrategySelector strategySelector,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool
  )
  {
    this.strategySelector = strategySelector;
    this.mergeBufferPool = mergeBufferPool;
  }

  /**
   * Prepares broker resources for executing the given query.
   *
   * @param query a query to be executed
   *
   * @return broker resource needed to execute the query
   *
   * @throws ResourceLimitExceededException if there isn't enough resources for query execution
   */
  public GroupByQueryBrokerResource prepare(GroupByQuery query)
  {
    final int requiredMergeBufferNum;
    if (strategySelector.useStrategyV2(query)) {
      final int groupByLayerNum = countGroupByLayers(query, 1);
      requiredMergeBufferNum = Math.min(2, groupByLayerNum - 1);
    } else {
      requiredMergeBufferNum = 0;
    }

    final ResourceHolder<List<ByteBuffer>> mergeBufferHolders = mergeBufferPool.drain(requiredMergeBufferNum);
    if (mergeBufferHolders.get().size() < requiredMergeBufferNum) {
      mergeBufferHolders.close();
      throw new ResourceLimitExceededException("Cannot acquire enough merge buffers");
    } else {
      return new GroupByQueryBrokerResource(mergeBufferHolders);
    }
  }

  private static int countGroupByLayers(Query query, int foundNum)
  {
    final DataSource dataSource = query.getDataSource();
    if (dataSource instanceof QueryDataSource) {
      return countGroupByLayers(((QueryDataSource) dataSource).getQuery(), foundNum + 1);
    } else {
      return foundNum;
    }
  }
}
