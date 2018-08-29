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

package org.apache.druid.sql.calcite.util;

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;

import java.util.List;
import java.util.concurrent.Executor;

public class TestServerInventoryView implements TimelineServerView
{
  private static final DruidServerMetadata DUMMY_SERVER = new DruidServerMetadata(
      "dummy",
      "dummy",
      null,
      0,
      ServerType.HISTORICAL,
      "dummy",
      0
  );
  private final List<DataSegment> segments;

  public TestServerInventoryView(List<DataSegment> segments)
  {
    this.segments = ImmutableList.copyOf(segments);
  }

  @Override
  public TimelineLookup<String, ServerSelector> getTimeline(DataSource dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
  {
    for (final DataSegment segment : segments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER, segment));
    }

    exec.execute(callback::segmentViewInitialized);
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
  {
    for (DataSegment segment : segments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER, segment));
    }

    exec.execute(callback::timelineInitialized);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    // Do nothing
  }
}
