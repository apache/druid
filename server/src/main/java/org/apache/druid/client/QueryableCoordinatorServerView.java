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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

/**
 * ServerView of coordinator for the state of segments being loaded in the cluster.
 */
@ManageLifecycle
public class QueryableCoordinatorServerView extends BrokerServerView implements CoordinatorTimeline
{
  private final FilteredServerInventoryView baseView;

  @Inject
  public QueryableCoordinatorServerView(
      final QueryToolChestWarehouse warehouse,
      final QueryWatcher queryWatcher,
      final @Smile ObjectMapper smileMapper,
      final @EscalatedClient HttpClient httpClient,
      FilteredServerInventoryView baseView,
      TierSelectorStrategy tierSelectorStrategy,
      ServiceEmitter emitter,
      CoordinatorSegmentWatcherConfig segmentWatcherConfig
  )
  {
    super(warehouse, queryWatcher, smileMapper, httpClient, baseView, tierSelectorStrategy, emitter, new BrokerSegmentWatcherConfig() {
      @Override
      public boolean isAwaitInitializationOnStart()
      {
        return segmentWatcherConfig.isAwaitInitializationOnStart();
      }
    });
    this.baseView = baseView;
  }

  @Override
  public VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource)
  {
    String table = Iterables.getOnlyElement(dataSource.getTableNames());
    synchronized (getLock()) {
      // build a new timeline?
      VersionedIntervalTimeline<String, ServerSelector> timeline = getTimelines().get(table);
      Collection<ServerSelector> x = timeline.iterateAllObjects();
      VersionedIntervalTimeline<String, SegmentLoadInfo> newTimeline = new VersionedIntervalTimeline<>(Comparator.naturalOrder());
      newTimeline.addAll(x.stream().map(v -> new VersionedIntervalTimeline.PartitionChunkEntry<String, SegmentLoadInfo>(
          v.getSegment().getInterval(), v.getSegment().getVersion(), v.getSegment().getShardSpec().createChunk(v.toSegmentLoadInfo()))).iterator());

      return newTimeline;
    }
  }

  @Override
  public Map<SegmentId, SegmentLoadInfo> getSegmentLoadInfos()
  {
    return CollectionUtils.mapValues(getSelectors(), ServerSelector::toSegmentLoadInfo);
  }

  @Override
  public DruidServer getInventoryValue(String serverKey)
  {
    return baseView.getInventoryValue(serverKey);
  }

  @Override
  public Collection<DruidServer> getInventory()
  {
    return baseView.getInventory();
  }

  @Override
  public boolean isStarted()
  {
    return baseView.isStarted();
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    return baseView.isSegmentLoadedByServer(serverKey, segment);
  }
}
