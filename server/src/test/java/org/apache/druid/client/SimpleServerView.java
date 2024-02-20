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
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * A simple broker server view for testing which you can manually update the server view.
 */
public class SimpleServerView implements TimelineServerView
{
  private static final QueryWatcher NOOP_QUERY_WATCHER = (query, future) -> {};
  private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
      new RandomServerSelectorStrategy()
  );
  // server -> queryRunner
  private final Map<DruidServer, QueryableDruidServer> servers = new HashMap<>();
  // segmentId -> serverSelector
  private final Map<String, ServerSelector> selectors = new HashMap<>();
  // dataSource -> version -> serverSelector
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

  private final DirectDruidClientFactory clientFactory;

  public SimpleServerView(
      QueryToolChestWarehouse warehouse,
      ObjectMapper objectMapper,
      HttpClient httpClient
  )
  {
    this.clientFactory = new DirectDruidClientFactory(
        new NoopServiceEmitter(),
        warehouse,
        NOOP_QUERY_WATCHER,
        objectMapper,
        httpClient
    );
  }

  public void addServer(DruidServer server, DataSegment dataSegment)
  {
    servers.put(server, new QueryableDruidServer<>(server, clientFactory.makeDirectClient(server)));
    addSegmentToServer(server, dataSegment);
  }

  public void removeServer(DruidServer server)
  {
    servers.remove(server);
  }

  public void unannounceSegmentFromServer(DruidServer server, DataSegment segment)
  {
    final QueryableDruidServer queryableDruidServer = servers.get(server);
    if (queryableDruidServer == null) {
      throw new ISE("Unknown server [%s]", server);
    }
    final ServerSelector selector = selectors.get(segment.getId().toString());
    if (selector == null) {
      throw new ISE("Unknown segment [%s]", segment.getId());
    }
    if (!selector.removeServer(queryableDruidServer)) {
      throw new ISE("Failed to remove segment[%s] from server[%s]", segment.getId(), server);
    }
    final VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
    if (timeline == null) {
      throw new ISE("Unknown datasource [%s]", segment.getDataSource());
    }
    timeline.remove(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
  }

  private void addSegmentToServer(DruidServer server, DataSegment segment)
  {
    final ServerSelector selector = selectors.computeIfAbsent(
        segment.getId().toString(),
        k -> new ServerSelector(segment, tierSelectorStrategy)
    );
    selector.addServerAndUpdateSegment(servers.get(server), segment);
    // broker needs to skip tombstones in its timelines
    timelines.computeIfAbsent(segment.getDataSource(), k -> new VersionedIntervalTimeline<>(Ordering.natural(), true))
             .add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
  }

  @Override
  public Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(DataSourceAnalysis analysis)
  {
    final TableDataSource table =
        analysis.getBaseTableDataSource()
                .orElseThrow(() -> new ISE("Cannot handle datasource: %s", analysis.getBaseDataSource()));

    return Optional.ofNullable(timelines.get(table.getName()));
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return Collections.emptyList();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    final QueryableDruidServer queryableDruidServer = Preconditions.checkNotNull(servers.get(server), "server");
    return (QueryRunner<T>) queryableDruidServer.getQueryRunner();
  }

  @Override
  public void registerTimelineCallback(Executor exec, TimelineCallback callback)
  {
    // do nothing
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    // do nothing
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    // do nothing
  }

  public static DruidServer createServer(int nameSuiffix)
  {
    return new DruidServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        Long.MAX_VALUE,
        ServerType.HISTORICAL,
        "default",
        0
    );
  }
}
