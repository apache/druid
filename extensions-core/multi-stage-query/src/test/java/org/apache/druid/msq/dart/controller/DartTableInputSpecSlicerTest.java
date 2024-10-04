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

package org.apache.druid.msq.dart.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.table.RichSegmentDescriptor;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DartTableInputSpecSlicerTest extends InitializedNullHandlingTest
{
  private static final String QUERY_ID = "abc";
  private static final String DATASOURCE = "test-ds";
  private static final String DATASOURCE_NONEXISTENT = "nonexistent-ds";
  private static final String PARTITION_DIM = "dim";
  private static final long BYTES_PER_SEGMENT = 1000;

  /**
   * List of servers, with descending priority, so earlier servers are preferred by the {@link ServerSelector}.
   * This makes tests deterministic.
   */
  private static final List<DruidServerMetadata> SERVERS = ImmutableList.of(
      new DruidServerMetadata("no", "localhost:1001", null, 1, ServerType.HISTORICAL, "__default", 2), // plaintext
      new DruidServerMetadata("no", null, "localhost:1002", 1, ServerType.HISTORICAL, "__default", 1), // TLS
      new DruidServerMetadata("no", "localhost:1003", null, 1, ServerType.REALTIME, "__default", 0)
  );

  /**
   * Dart {@link WorkerId} derived from {@link #SERVERS}.
   */
  private static final List<String> WORKER_IDS =
      SERVERS.stream()
             .map(server -> new WorkerId("http", server.getHost(), QUERY_ID).toString())
             .collect(Collectors.toList());

  /**
   * Segment that is one of two in a range-partitioned time chunk.
   */
  private static final DataSegment SEGMENT1 = new DataSegment(
      DATASOURCE,
      Intervals.of("2000/2001"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new DimensionRangeShardSpec(ImmutableList.of(PARTITION_DIM), null, new StringTuple(new String[]{"foo"}), 0, 2),
      null,
      null,
      BYTES_PER_SEGMENT
  );

  /**
   * Segment that is one of two in a range-partitioned time chunk.
   */
  private static final DataSegment SEGMENT2 = new DataSegment(
      DATASOURCE,
      Intervals.of("2000/2001"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new DimensionRangeShardSpec(ImmutableList.of("dim"), new StringTuple(new String[]{"foo"}), null, 1, 2),
      null,
      null,
      BYTES_PER_SEGMENT
  );

  /**
   * Segment that is alone in a time chunk. It is not served by any server, and such segments are assigned to the
   * existing servers round-robin. Because this is the only "not served by any server" segment, it should
   * be assigned to the first server.
   */
  private static final DataSegment SEGMENT3 = new DataSegment(
      DATASOURCE,
      Intervals.of("2001/2002"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new NumberedShardSpec(0, 1),
      null,
      null,
      BYTES_PER_SEGMENT
  );

  /**
   * Segment that should be ignored because it's a tombstone.
   */
  private static final DataSegment SEGMENT4 = new DataSegment(
      DATASOURCE,
      Intervals.of("2002/2003"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      TombstoneShardSpec.INSTANCE,
      null,
      null,
      BYTES_PER_SEGMENT
  );

  /**
   * Segment that should be ignored (for now) because it's realtime-only.
   */
  private static final DataSegment SEGMENT5 = new DataSegment(
      DATASOURCE,
      Intervals.of("2003/2004"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new NumberedShardSpec(0, 1),
      null,
      null,
      BYTES_PER_SEGMENT
  );

  /**
   * Mapping of segment to servers (indexes in {@link #SERVERS}).
   */
  private static final Map<DataSegment, IntList> SEGMENT_SERVERS =
      ImmutableMap.<DataSegment, IntList>builder()
                  .put(SEGMENT1, IntList.of(0))
                  .put(SEGMENT2, IntList.of(1))
                  .put(SEGMENT3, IntLists.emptyList())
                  .put(SEGMENT4, IntList.of(1))
                  .put(SEGMENT5, IntList.of(2))
                  .build();

  private AutoCloseable mockCloser;

  /**
   * Slicer under test. Built using {@link #timeline} and {@link #SERVERS}.
   */
  private DartTableInputSpecSlicer slicer;

  /**
   * Timeline built from {@link #SEGMENT_SERVERS} and {@link #SERVERS}.
   */
  private VersionedIntervalTimeline<String, ServerSelector> timeline;

  /**
   * Server view that uses {@link #timeline}.
   */
  @Mock
  private TimelineServerView serverView;

  @BeforeEach
  void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    slicer = DartTableInputSpecSlicer.createFromWorkerIds(WORKER_IDS, serverView);

    // Add all segments to the timeline, round-robin across the two servers.
    timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    for (Map.Entry<DataSegment, IntList> entry : SEGMENT_SERVERS.entrySet()) {
      final DataSegment dataSegment = entry.getKey();
      final IntList segmentServers = entry.getValue();
      final ServerSelector serverSelector = new ServerSelector(
          dataSegment,
          new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
      );
      for (int serverNumber : segmentServers) {
        final DruidServerMetadata serverMetadata = SERVERS.get(serverNumber);
        final DruidServer server = new DruidServer(
            serverMetadata.getName(),
            serverMetadata.getHostAndPort(),
            serverMetadata.getHostAndTlsPort(),
            serverMetadata.getMaxSize(),
            serverMetadata.getType(),
            serverMetadata.getTier(),
            serverMetadata.getPriority()
        );
        serverSelector.addServerAndUpdateSegment(new QueryableDruidServer<>(server, null), dataSegment);
      }
      timeline.add(
          dataSegment.getInterval(),
          dataSegment.getVersion(),
          dataSegment.getShardSpec().createChunk(serverSelector)
      );
    }

    Mockito.when(serverView.getDruidServerMetadatas()).thenReturn(SERVERS);
    Mockito.when(serverView.getTimeline(new TableDataSource(DATASOURCE).getAnalysis()))
           .thenReturn(Optional.of(timeline));
    Mockito.when(serverView.getTimeline(new TableDataSource(DATASOURCE_NONEXISTENT).getAnalysis()))
           .thenReturn(Optional.empty());
  }

  @AfterEach
  void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_sliceDynamic()
  {
    // This slicer cannot sliceDynamic.

    final TableInputSpec inputSpec = new TableInputSpec(DATASOURCE, null, null, null);
    Assertions.assertFalse(slicer.canSliceDynamic(inputSpec));
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> slicer.sliceDynamic(inputSpec, 1, 1, 1)
    );
  }

  @Test
  public void test_sliceStatic_wholeTable_oneSlice()
  {
    // When 1 slice is requested, all segments are assigned to one server, even if that server doesn't actually
    // currently serve those segments.

    final TableInputSpec inputSpec = new TableInputSpec(DATASOURCE, null, null, null);
    final List<InputSlice> inputSlices = slicer.sliceStatic(inputSpec, 1);
    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT3.getInterval(),
                        SEGMENT3.getInterval(),
                        SEGMENT3.getVersion(),
                        SEGMENT3.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        inputSlices
    );
  }

  @Test
  public void test_sliceStatic_wholeTable_twoSlices()
  {
    // When 2 slices are requested, we assign segments to the servers that have those segments.

    final TableInputSpec inputSpec = new TableInputSpec(DATASOURCE, null, null, null);
    final List<InputSlice> inputSlices = slicer.sliceStatic(inputSpec, 2);
    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT3.getInterval(),
                        SEGMENT3.getInterval(),
                        SEGMENT3.getVersion(),
                        SEGMENT3.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        inputSlices
    );
  }

  @Test
  public void test_sliceStatic_wholeTable_threeSlices()
  {
    // When 3 slices are requested, only 2 are returned, because we only have two workers.

    final TableInputSpec inputSpec = new TableInputSpec(DATASOURCE, null, null, null);
    final List<InputSlice> inputSlices = slicer.sliceStatic(inputSpec, 3);
    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT3.getInterval(),
                        SEGMENT3.getInterval(),
                        SEGMENT3.getVersion(),
                        SEGMENT3.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            ),
            NilInputSlice.INSTANCE
        ),
        inputSlices
    );
  }

  @Test
  public void test_sliceStatic_nonexistentTable()
  {
    final TableInputSpec inputSpec = new TableInputSpec(DATASOURCE_NONEXISTENT, null, null, null);
    final List<InputSlice> inputSlices = slicer.sliceStatic(inputSpec, 1);
    Assertions.assertEquals(
        Collections.emptyList(),
        inputSlices
    );
  }

  @Test
  public void test_sliceStatic_dimensionFilter_twoSlices()
  {
    // Filtered on a dimension that is used for range partitioning in 2000/2001, so one segment gets pruned out.

    final TableInputSpec inputSpec = new TableInputSpec(
        DATASOURCE,
        null,
        new EqualityFilter(PARTITION_DIM, ColumnType.STRING, "abc", null),
        null
    );

    final List<InputSlice> inputSlices = slicer.sliceStatic(inputSpec, 2);

    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT3.getInterval(),
                        SEGMENT3.getInterval(),
                        SEGMENT3.getVersion(),
                        SEGMENT3.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            ),
            NilInputSlice.INSTANCE
        ),
        inputSlices
    );
  }

  @Test
  public void test_sliceStatic_timeFilter_twoSlices()
  {
    // Filtered on 2000/2001, so other segments get pruned out.

    final TableInputSpec inputSpec = new TableInputSpec(
        DATASOURCE,
        Collections.singletonList(Intervals.of("2000/P1Y")),
        null,
        null
    );

    final List<InputSlice> inputSlices = slicer.sliceStatic(inputSpec, 2);

    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        SEGMENT1.getInterval(),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            ),
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        SEGMENT2.getInterval(),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        inputSlices
    );
  }
}
