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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.LoadScope;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.groupby.GroupByStatsProvider;

import java.nio.ByteBuffer;

@LoadScope(roles = {
    NodeRole.BROKER_JSON_NAME,
    NodeRole.HISTORICAL_JSON_NAME,
    NodeRole.INDEXER_JSON_NAME,
    NodeRole.PEON_JSON_NAME
})
public class GroupByStatsMonitor extends AbstractMonitor
{
  private final GroupByStatsProvider groupByStatsProvider;
  private final BlockingPool<ByteBuffer> mergeBufferPool;

  @Inject
  public GroupByStatsMonitor(
      GroupByStatsProvider groupByStatsProvider,
      @Merging BlockingPool<ByteBuffer> mergeBufferPool
  )
  {
    this.groupByStatsProvider = groupByStatsProvider;
    this.mergeBufferPool = mergeBufferPool;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();

    emitter.emit(builder.setMetric("mergeBuffer/pendingRequests", mergeBufferPool.getPendingRequests()));

    emitter.emit(builder.setMetric("mergeBuffer/used", mergeBufferPool.getUsedResourcesCount()));

    GroupByStatsProvider.AggregateStats statsContainer = groupByStatsProvider.getStatsSince();

    if (statsContainer.getMergeBufferQueries() > 0) {
      emitter.emit(builder.setMetric("mergeBuffer/queries", statsContainer.getMergeBufferQueries()));
      emitter.emit(builder.setMetric(
          "mergeBuffer/acquisitionTimeNs",
          statsContainer.getMergeBufferAcquisitionTimeNs()
      ));
    }

    if (statsContainer.getSpilledQueries() > 0) {
      emitter.emit(builder.setMetric("groupBy/spilledQueries", statsContainer.getSpilledQueries()));
      emitter.emit(builder.setMetric("groupBy/spilledBytes", statsContainer.getSpilledBytes()));
    }

    if (statsContainer.getMergeDictionarySize() > 0) {
      emitter.emit(builder.setMetric("groupBy/mergeDictionarySize", statsContainer.getMergeDictionarySize()));
    }

    return true;
  }
}
