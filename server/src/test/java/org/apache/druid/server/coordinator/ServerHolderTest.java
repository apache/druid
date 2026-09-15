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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ServerHolderTest
{
  private static final List<DataSegment> SEGMENTS = ImmutableList.of(
      new DataSegment(
          "src1",
          Intervals.of("2015-04-12/2015-04-13"),
          "1",
          ImmutableMap.of("containerName", "container1", "blobPath", "blobPath1"),
          null,
          null,
          NoneShardSpec.instance(),
          0,
          1
      ),
      new DataSegment(
          "src2",
          Intervals.of("2015-04-12/2015-04-13"),
          "1",
          ImmutableMap.of("containerName", "container2", "blobPath", "blobPath2"),
          null,
          null,
          NoneShardSpec.instance(),
          0,
          1
      )
  );

  private static final Map<String, ImmutableDruidDataSource> DATA_SOURCES = ImmutableMap.of(
      "src1", new ImmutableDruidDataSource("src1", Collections.emptyMap(), Collections.singletonList(SEGMENTS.get(0))),
      "src2", new ImmutableDruidDataSource("src2", Collections.emptyMap(), Collections.singletonList(SEGMENTS.get(1)))
  );

  private static final long SEGMENT_SIZE = 1000L;

  private static final Map<String, Object> PARTIAL_LOAD_SPEC =
      ImmutableMap.of("type", "partialProjection", "fingerprint", "v1:abc");

  /**
   * Non-zero sized counterparts of {@link #SEGMENTS}, for the projection accounting tests.
   */
  private static final List<DataSegment> SIZED_SEGMENTS = ImmutableList.of(
      DataSegment.builder(SEGMENTS.get(0)).size(SEGMENT_SIZE).build(),
      DataSegment.builder(SEGMENTS.get(1)).size(SEGMENT_SIZE).build()
  );

  @Test
  public void testCompareTo()
  {
    // available size of 100
    final ServerHolder h1 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, null, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    // available size of 100
    final ServerHolder h2 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 200L, null, ServerType.HISTORICAL, "tier1", 0),
            100L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    // available size of 10
    final ServerHolder h3 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 1000L, null, ServerType.HISTORICAL, "tier1", 0),
            990L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    // available size of 50
    final ServerHolder h4 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 50L, null, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    Assertions.assertEquals(0, h1.compareTo(h2));
    Assertions.assertEquals(1, h3.compareTo(h1));
    Assertions.assertEquals(1, h3.compareTo(h4));
  }

  @Test
  public void testEquals()
  {
    final ServerHolder h1 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, null, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    final ServerHolder h2 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name2", "host1", null, 200L, null, ServerType.HISTORICAL, "tier1", 0),
            100L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    final ServerHolder h3 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host2", null, 200L, null, ServerType.HISTORICAL, "tier1", 0),
            100L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    final ServerHolder h4 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 200L, null, ServerType.HISTORICAL, "tier2", 0),
            100L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    final ServerHolder h5 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, null, ServerType.REALTIME, "tier1", 0),
            0L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );

    Assertions.assertEquals(h1, h2);
    Assertions.assertNotEquals(h1, h3);
    Assertions.assertNotEquals(h1, h4);
    Assertions.assertNotEquals(h1, h5);
  }

  @Test
  public void testIsServingSegment()
  {
    final ServerHolder h1 = new ServerHolder(
        new ImmutableDruidServer(
            new DruidServerMetadata("name1", "host1", null, 100L, null, ServerType.HISTORICAL, "tier1", 0),
            0L,
            ImmutableMap.of("src1", DATA_SOURCES.get("src1")),
            1
        ),
        new TestLoadQueuePeon()
    );
    Assertions.assertTrue(h1.isServingSegment(SEGMENTS.get(0)));
    Assertions.assertFalse(h1.isServingSegment(SEGMENTS.get(1)));
    Assertions.assertFalse(h1.isLoadQueueFull());
  }

  @Test
  public void testLoadOfAbsentSegmentProjectsItsFullSize()
  {
    final ServerHolder holder = holderServing(null);
    final long sizeUsedBefore = holder.getSizeUsed();
    final int projectedCountBefore = holder.getProjectedSegmentCounts().getTotalSegmentCount();

    Assertions.assertTrue(holder.startOperation(SegmentAction.LOAD, SIZED_SEGMENTS.get(1)));

    Assertions.assertEquals(sizeUsedBefore + SEGMENT_SIZE, holder.getSizeUsed());
    Assertions.assertEquals(
        projectedCountBefore + 1,
        holder.getProjectedSegmentCounts().getTotalSegmentCount()
    );
  }

  @Test
  public void testInPlaceReloadOfPartialReplicaProjectsOnlyTheDelta()
  {
    // A partial replica announces its realized footprint as curr_size, so reloading it in place can add at most the
    // rest of the segment. Counting the whole segment again would double count the bytes already on disk.
    final ServerHolder holder = holderServing(PartialLoadProfile.forLoaded(PARTIAL_LOAD_SPEC, "v1:abc", 400L));
    final long sizeUsedBefore = holder.getSizeUsed();
    final int projectedCountBefore = holder.getProjectedSegmentCounts().getTotalSegmentCount();

    Assertions.assertTrue(holder.startOperation(SegmentAction.LOAD, SIZED_SEGMENTS.get(0)));

    Assertions.assertEquals(sizeUsedBefore + (SEGMENT_SIZE - 400L), holder.getSizeUsed());
    Assertions.assertEquals(
        projectedCountBefore,
        holder.getProjectedSegmentCounts().getTotalSegmentCount(),
        "an in-place reload refreshes a replica that is already projected, it does not add one"
    );
  }

  @Test
  public void testInPlaceReloadOfFullReplicaProjectsNothing()
  {
    // A replica with no profile is a regular full load that already holds the whole segment, so applying a
    // partial-load rule to it (or reverting it back to a plain load spec) cannot add any bytes.
    final ServerHolder holder = holderServing(null);
    final long sizeUsedBefore = holder.getSizeUsed();

    Assertions.assertTrue(holder.startOperation(SegmentAction.LOAD, SIZED_SEGMENTS.get(0)));

    Assertions.assertEquals(sizeUsedBefore, holder.getSizeUsed());
    Assertions.assertEquals(1, holder.getProjectedSegmentCounts().getTotalSegmentCount());
  }

  @Test
  public void testCancellingAnInPlaceReloadRestoresTheProjection()
  {
    // add/remove have to agree on the delta, otherwise a cancelled reload leaves the server's projection skewed for
    // the rest of the run.
    final ServerHolder holder = holderServing(PartialLoadProfile.forLoaded(PARTIAL_LOAD_SPEC, "v1:abc", 400L));
    final long sizeUsedBefore = holder.getSizeUsed();
    final int projectedCountBefore = holder.getProjectedSegmentCounts().getTotalSegmentCount();

    // Queue through the load queue manager rather than startOperation directly, so that the peon holds the segment
    // and can accept the cancellation.
    Assertions.assertTrue(
        new SegmentLoadQueueManager(null, null)
            .loadSegment(SIZED_SEGMENTS.get(0), holder, SegmentAction.LOAD, null)
    );
    Assertions.assertTrue(holder.cancelOperation(SegmentAction.LOAD, SIZED_SEGMENTS.get(0)));

    Assertions.assertEquals(sizeUsedBefore, holder.getSizeUsed());
    Assertions.assertEquals(
        projectedCountBefore,
        holder.getProjectedSegmentCounts().getTotalSegmentCount()
    );
  }

  /**
   * A historical serving {@code SIZED_SEGMENTS.get(0)}, announced with {@code profile} when it is non-null so that the
   * server's curr_size reflects a partial footprint rather than the whole segment.
   */
  private static ServerHolder holderServing(@Nullable PartialLoadProfile profile)
  {
    final DruidServer server =
        new DruidServer("name1", "host1", null, 10_000L, null, ServerType.HISTORICAL, "tier1", 0);
    server.addDataSegment(SIZED_SEGMENTS.get(0), profile);
    return new ServerHolder(server.toImmutableDruidServer(), new TestLoadQueuePeon());
  }
}
