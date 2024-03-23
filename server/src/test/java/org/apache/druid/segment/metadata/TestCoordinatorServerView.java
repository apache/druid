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

package org.apache.druid.segment.metadata;

import com.google.common.collect.Lists;
import org.apache.druid.client.CoordinatorSegmentWatcherConfig;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DirectDruidClientFactory;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.SegmentLoadInfo;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.easymock.EasyMock;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class TestCoordinatorServerView extends CoordinatorServerView
{
  private static final DruidServer DUMMY_SERVER = new DruidServer(
      "dummy",
      "dummy",
      null,
      0,
      ServerType.HISTORICAL,
      "dummy",
      0
  );
  private static final DruidServer DUMMY_SERVER_REALTIME = new DruidServer(
      "dummy2",
      "dummy2",
      null,
      0,
      ServerType.INDEXER_EXECUTOR,
      "dummy",
      0
  );
  private static final DruidServer DUMMY_BROKER = new DruidServer(
      "dummy3",
      "dummy3",
      null,
      0,
      ServerType.BROKER,
      "dummy",
      0
  );

  private final Map<DataSource, VersionedIntervalTimeline<String, SegmentLoadInfo>> timelines;
  private Map<Pair<DataSegment, ServerType>, Pair<DruidServerMetadata, SegmentLoadInfo>> segmentInfo;
  private List<DataSegment> segments = new ArrayList<>();
  private List<DataSegment> realtimeSegments = new ArrayList<>();
  private List<DataSegment> brokerSegments = new ArrayList<>();
  private List<Pair<Executor, TimelineServerView.TimelineCallback>> timelineCallbackExecs = new ArrayList<>();

  public TestCoordinatorServerView(List<DataSegment> segments, List<DataSegment> realtimeSegments)
  {
    super(
        Mockito.mock(ServerInventoryView.class),
        Mockito.mock(CoordinatorSegmentWatcherConfig.class),
        Mockito.mock(ServiceEmitter.class),
        Mockito.mock(DirectDruidClientFactory.class)
    );

    timelines = new HashMap<>();
    segmentInfo = new HashMap<>();

    for (DataSegment segment : segments) {
      addToTimeline(segment, DUMMY_SERVER);
    }

    for (DataSegment realtimeSegment : realtimeSegments) {
      addToTimeline(realtimeSegment, DUMMY_SERVER_REALTIME);
    }
  }

  private DruidServer getServerForType(ServerType serverType)
  {
    switch (serverType) {
      case BROKER:
        return DUMMY_BROKER;
      case INDEXER_EXECUTOR:
        return DUMMY_SERVER_REALTIME;
      default:
        return DUMMY_SERVER;
    }
  }

  private void addToTimeline(DataSegment dataSegment, DruidServer druidServer)
  {
    if (druidServer.getMetadata().getType() == ServerType.INDEXER_EXECUTOR) {
      realtimeSegments.add(dataSegment);
    } else if (druidServer.getMetadata().getType() == ServerType.BROKER) {
      brokerSegments.add(dataSegment);
    } else {
      segments.add(dataSegment);
    }
    SegmentDescriptor segmentDescriptor = dataSegment.getId().toDescriptor();
    SegmentLoadInfo segmentLoadInfo = new SegmentLoadInfo(dataSegment);
    segmentLoadInfo.addServer(druidServer.getMetadata());

    segmentInfo.put(Pair.of(dataSegment, druidServer.getType()), Pair.of(druidServer.getMetadata(), segmentLoadInfo));

    TableDataSource tableDataSource = new TableDataSource(dataSegment.getDataSource());
    timelines.computeIfAbsent(tableDataSource, value -> new VersionedIntervalTimeline<>(Comparator.naturalOrder()));
    VersionedIntervalTimeline<String, SegmentLoadInfo> timeline = timelines.get(tableDataSource);
    final ShardSpec shardSpec = new SingleDimensionShardSpec("dimAll", null, null, 0, 1);
    timeline.add(dataSegment.getInterval(), segmentDescriptor.getVersion(), shardSpec.createChunk(segmentLoadInfo));
  }

  @Override
  public QueryRunner getQueryRunner(String serverName)
  {
    return EasyMock.mock(QueryRunner.class);
  }

  @Override
  public VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource)
  {
    return timelines.get(dataSource);
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineServerView.TimelineCallback callback)
  {
    for (DataSegment segment : segments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER.getMetadata(), segment));
    }
    for (DataSegment segment : realtimeSegments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER_REALTIME.getMetadata(), segment));
    }
    exec.execute(callback::timelineInitialized);
    timelineCallbackExecs.add(new Pair<>(exec, callback));
  }

  public void addSegment(DataSegment segment, ServerType serverType)
  {
    DruidServer druidServer = getServerForType(serverType);
    addToTimeline(segment, druidServer);

    timelineCallbackExecs.forEach(
        execAndCallback -> execAndCallback.lhs.execute(() -> execAndCallback.rhs.segmentAdded(druidServer.getMetadata(), segment))
    );
  }

  public void removeSegment(DataSegment segment, ServerType serverType)
  {
    DruidServerMetadata druidServerMetadata;
    if (serverType == ServerType.BROKER) {
      druidServerMetadata = DUMMY_BROKER.getMetadata();
      brokerSegments.remove(segment);
    } else if (serverType == ServerType.INDEXER_EXECUTOR) {
      druidServerMetadata = DUMMY_SERVER_REALTIME.getMetadata();
      realtimeSegments.remove(segment);
    } else {
      druidServerMetadata = DUMMY_SERVER.getMetadata();
      segments.remove(segment);
    }

    Pair<DataSegment, ServerType> key = Pair.of(segment, serverType);
    Pair<DruidServerMetadata, SegmentLoadInfo> info = segmentInfo.get(key);

    segmentInfo.remove(key);

    if (null != info) {
      timelines.get(new TableDataSource(segment.getDataSource())).remove(
          segment.getInterval(),
          "0",
          new SingleDimensionShardSpec("dimAll", null, null, 0, 1)
              .createChunk(info.rhs)
      );
    }

    timelineCallbackExecs.forEach(
        execAndCallback -> execAndCallback.lhs.execute(() -> {
          execAndCallback.rhs.serverSegmentRemoved(druidServerMetadata, segment);

          // Fire segmentRemoved if all replicas have been removed.
          if (!segments.contains(segment) && !brokerSegments.contains(segment) && !realtimeSegments.remove(segment)) {
            execAndCallback.rhs.segmentRemoved(segment);
          }
        })
    );
  }

  public void addSegmentSchemas(SegmentSchemas segmentSchemas)
  {
    timelineCallbackExecs.forEach(
        execAndCallback -> execAndCallback.lhs.execute(() -> execAndCallback.rhs.segmentSchemasAnnounced(segmentSchemas))
    );
  }

  @Nullable
  @Override
  public List<DruidServer> getInventory()
  {
    return Lists.newArrayList(DUMMY_SERVER, DUMMY_SERVER_REALTIME);
  }

  public List<DataSegment> getSegmentsOfServer(DruidServer druidServer)
  {
    if (druidServer.getType() == ServerType.BROKER) {
      return Lists.newArrayList(brokerSegments);
    } else if (druidServer.getType() == ServerType.INDEXER_EXECUTOR) {
      return Lists.newArrayList(realtimeSegments);
    } else {
      return Lists.newArrayList(segments);
    }
  }
}
