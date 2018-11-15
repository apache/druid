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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class TestTimelineServerView implements TimelineServerView
{
  private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
      new RandomServerSelectorStrategy()
  );
  // server -> queryRunner
  private final Map<DruidServer, TestDruidServer> servers = new HashMap<>();
  // segmentId -> serverSelector
  private final Map<String, ServerSelector> selectors = new HashMap<>();
  // dataSource -> version -> serverSelector
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

  void addServer(DruidServer server, TestQueryRunner queryRunner)
  {
    servers.put(server, new TestDruidServer(server, queryRunner));
  }

  void addSegmentToServer(DruidServer server, DataSegment segment)
  {
    final ServerSelector selector = selectors.computeIfAbsent(
        segment.getIdentifier(),
        k -> new ServerSelector(segment, tierSelectorStrategy)
    );
    selector.addServerAndUpdateSegment(servers.get(server), segment);
    timelines.computeIfAbsent(segment.getDataSource(), k -> new VersionedIntervalTimeline<>(Ordering.natural()))
             .add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
  }

  @Nullable
  @Override
  public TimelineLookup<String, ServerSelector> getTimeline(DataSource dataSource)
  {
    final String table = Iterables.getOnlyElement(dataSource.getNames());
    return timelines.get(table);
  }

  @Override
  public List<ImmutableDruidServer> getDruidServers()
  {
    return Collections.emptyList();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    final TestDruidServer queryableDruidServer = Preconditions.checkNotNull(servers.get(server), "server");
    return (QueryRunner<T>) queryableDruidServer.queryRunner;
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

  private static class TestDruidServer implements QueryableDruidServer<TestQueryRunner>
  {
    private final DruidServer server;
    private final TestQueryRunner queryRunner;

    TestDruidServer(DruidServer server, TestQueryRunner queryRunner)
    {
      this.server = server;
      this.queryRunner = queryRunner;
    }

    @Override
    public DruidServer getServer()
    {
      return server;
    }

    @Override
    public TestQueryRunner getQueryRunner()
    {
      return queryRunner;
    }
  }
}
