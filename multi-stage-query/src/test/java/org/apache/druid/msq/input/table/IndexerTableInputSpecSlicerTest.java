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

package org.apache.druid.msq.input.table;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.indexing.IndexerTableInputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.FilterSegmentPruner;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SegmentPruner;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentDetail;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class IndexerTableInputSpecSlicerTest extends InitializedNullHandlingTest
{
  private static final String DATASOURCE = "test-ds";
  private static final long BYTES_PER_SEGMENT = 1000;

  private static final DataSegment SEGMENT1 = DataSegment.builder(SegmentId.of(DATASOURCE, Intervals.of("2000/2001"), "1", 0))
                                                         .loadSpec(Collections.emptyMap())
                                                         .dimensions(Collections.emptyList())
                                                         .metrics(Collections.emptyList())
                                                         .shardSpec(new DimensionRangeShardSpec(
                                                                 ImmutableList.of("dim"),
                                                                 VirtualColumns.EMPTY,
                                                                 null,
                                                                 new StringTuple(new String[]{"foo"}),
                                                                 0,
                                                                 2
                                                             ))
                                                         .binaryVersion(null)
                                                         .size(BYTES_PER_SEGMENT)
                                                         .build();

  private static final DataSegment SEGMENT2 = DataSegment.builder(SegmentId.of(DATASOURCE, Intervals.of("2000/2001"), "1", 1))
                                                         .loadSpec(Collections.emptyMap())
                                                         .dimensions(Collections.emptyList())
                                                         .metrics(Collections.emptyList())
                                                         .shardSpec(new DimensionRangeShardSpec(
                                                                 ImmutableList.of("dim"),
                                                                 VirtualColumns.EMPTY,
                                                                 new StringTuple(new String[]{"foo"}),
                                                                 null,
                                                                 1,
                                                                 2
                                                             ))
                                                         .binaryVersion(null)
                                                         .size(BYTES_PER_SEGMENT)
                                                         .build();

  private static final DataSegment SEGMENT3 = DataSegment.builder(SegmentId.of(DATASOURCE, Intervals.of("2001/2002"), "1", 0))
                                                         .loadSpec(Collections.emptyMap())
                                                         .dimensions(Collections.emptyList())
                                                         .metrics(Collections.emptyList())
                                                         .shardSpec(TombstoneShardSpec.INSTANCE)
                                                         .binaryVersion(null)
                                                         .size(BYTES_PER_SEGMENT)
                                                         .build();
  private static final String CLUSTERED_DATASOURCE = "clustered-ds";
  private static final RowSignature CLUSTERING_COLUMNS = RowSignature.builder()
                                                                     .add("tenant", ColumnType.STRING)
                                                                     .add("region", ColumnType.STRING)
                                                                     .build();

  private static final DataSegment CLUSTERED_SEGMENT1 = makeClusteredSegment(
      0,
      null,
      StringTuple.create("acme", "us-west-3"),
      List.of(List.of("acme", "us-west-1"), List.of("acme", "us-west-2"))
  );
  private static final DataSegment CLUSTERED_SEGMENT2 = makeClusteredSegment(
      1,
      StringTuple.create("acme", "us-west-3"),
      StringTuple.create("globex", "us-east-1"),
      List.of(List.of("acme", "us-west-3"), List.of("acme", "us-west-4"))
  );
  private static final DataSegment CLUSTERED_SEGMENT3 = makeClusteredSegment(
      2,
      StringTuple.create("globex", "us-east-1"),
      null,
      List.of(List.of("globex", "us-east-1"))
  );

  private SegmentTimeline timeline;
  private SegmentTimeline clusteredTimeline;
  private IndexerTableInputSpecSlicer slicer;
  private TaskActionClient taskActionClient;
  private RetrieveUsedSegmentsAction lastRetrieveUsedSegmentsAction;

  @BeforeEach
  public void setUp()
  {
    timeline = SegmentTimeline.forSegments(ImmutableList.of(SEGMENT1, SEGMENT2, SEGMENT3));
    clusteredTimeline = SegmentTimeline.forSegments(
        ImmutableList.of(CLUSTERED_SEGMENT1, CLUSTERED_SEGMENT2, CLUSTERED_SEGMENT3)
    );
    taskActionClient = new TaskActionClient()
    {
      @Override
      @SuppressWarnings("unchecked")
      public <RetType> RetType submit(TaskAction<RetType> taskAction)
      {
        if (taskAction instanceof RetrieveUsedSegmentsAction) {
          final RetrieveUsedSegmentsAction retrieveUsedSegmentsAction = (RetrieveUsedSegmentsAction) taskAction;
          lastRetrieveUsedSegmentsAction = retrieveUsedSegmentsAction;
          final String dataSource = retrieveUsedSegmentsAction.getDataSource();
          final SegmentTimeline dataSourceTimeline;

          if (DATASOURCE.equals(dataSource)) {
            dataSourceTimeline = timeline;
          } else if (CLUSTERED_DATASOURCE.equals(dataSource)) {
            dataSourceTimeline = clusteredTimeline;
          } else {
            return (RetType) Collections.emptyList();
          }

          // Like the Overlord, return only the segment details the action asks for.
          return (RetType) FluentIterable
              .from(retrieveUsedSegmentsAction.getIntervals())
              .transformAndConcat(
                  interval ->
                      VersionedIntervalTimeline.getAllObjects(dataSourceTimeline.lookup(interval))
              )
              .transform(segment -> segment.retainOnlyDetails(retrieveUsedSegmentsAction.getDetails()))
              .toList();
        }

        throw new UnsupportedOperationException();
      }
    };

    slicer = new IndexerTableInputSpecSlicer(
        null /* not used for SegmentSource.NONE */,
        taskActionClient,
        SegmentSource.NONE
    );
  }

  @Test
  public void test_canSliceDynamic()
  {
    Assertions.assertTrue(slicer.canSliceDynamic(new TableInputSpec(DATASOURCE, null, null)));
  }

  @Test
  public void test_sliceStatic_noDataSource()
  {
    final TableInputSpec spec = new TableInputSpec("no such datasource", null, null);
    Assertions.assertEquals(
        ImmutableList.of(NilInputSlice.INSTANCE, NilInputSlice.INSTANCE),
        slicer.sliceStatic(spec, null, 2)
    );
  }

  @Test
  public void test_sliceStatic_intervalFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(
            Intervals.of("2000/P1M"),
            Intervals.of("2000-06-01/P1M")
        ),
        null
    );

    Assertions.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000-06-01/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000-06-01/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceStatic(spec, null, 1)
    );
  }

  @Test
  public void test_sliceStatic_intervalFilterMatchesNothing()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        Collections.singletonList(Intervals.of("2002/P1M")),
        null
    );

    Assertions.assertEquals(
        ImmutableList.of(NilInputSlice.INSTANCE, NilInputSlice.INSTANCE),
        slicer.sliceStatic(spec, null, 2)
    );
  }

  @Test
  public void test_sliceStatic_segmentFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        null,
        List.of(new SegmentDescriptor(
            SEGMENT1.getInterval(),
            SEGMENT1.getVersion(),
            SEGMENT1.getShardSpec().getPartitionNum()
        ))
    );

    RichSegmentDescriptor expectedSegment = new RichSegmentDescriptor(
        SEGMENT1.getInterval(),
        SEGMENT1.getInterval(),
        SEGMENT1.getVersion(),
        SEGMENT1.getShardSpec().getPartitionNum()
    );
    Assertions.assertEquals(
        List.of(new SegmentsInputSlice(DATASOURCE, List.of(expectedSegment), List.of())),
        slicer.sliceStatic(spec, null, 1));
  }

  @Test
  public void test_sliceStatic_segmentAndIntervalFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        List.of(Intervals.of("2002/P1M")),
        List.of(new SegmentDescriptor(
            SEGMENT1.getInterval(),
            SEGMENT1.getVersion(),
            SEGMENT1.getShardSpec().getPartitionNum()
        ))
    );

    Assertions.assertEquals(
        ImmutableList.of(NilInputSlice.INSTANCE, NilInputSlice.INSTANCE),
        slicer.sliceStatic(spec, null, 2)
    );
  }

  @Test
  public void test_sliceStatic_dimFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        null,
        null
    );
    final FilterSegmentPruner pruner = new FilterSegmentPruner(
        new EqualityFilter("dim", ColumnType.STRING, "bar", null),
        null,
        null
    );

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
            NilInputSlice.INSTANCE
        ),
        slicer.sliceStatic(spec, pruner, 2)
    );
  }

  @Test
  public void test_sliceStatic_dimFilterNotUsed()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        null,
        null
    );
    final FilterSegmentPruner segmentPruner = new FilterSegmentPruner(
        new EqualityFilter("dim", ColumnType.STRING, "bar", null),
        Collections.emptySet(),
        null
    );

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
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceStatic(spec, segmentPruner, 1)
    );
  }

  @Test
  public void test_sliceStatic_intervalAndDimFilter()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(
            Intervals.of("2000/P1M"),
            Intervals.of("2000-06-01/P1M")
        ),
        null
    );
    final FilterSegmentPruner segmentPruner = new FilterSegmentPruner(
        new EqualityFilter("dim", ColumnType.STRING, "bar", null),
        null,
        null
    );

    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
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
                        SEGMENT1.getInterval(),
                        Intervals.of("2000-06-01/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceStatic(spec, segmentPruner, 2)
    );
  }

  @Test
  public void test_sliceStatic_oneSlice()
  {
    final TableInputSpec spec = new TableInputSpec(DATASOURCE, null, null);
    Assertions.assertEquals(
        Collections.singletonList(
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
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceStatic(spec, null, 1)
    );
  }

  @Test
  public void test_sliceStatic_needTwoSlices()
  {
    final TableInputSpec spec = new TableInputSpec(DATASOURCE, null, null);
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
        slicer.sliceStatic(spec, null, 2)
    );
  }

  @Test
  public void test_sliceStatic_threeSlices()
  {
    final TableInputSpec spec = new TableInputSpec(DATASOURCE, null, null);
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
            ),
            NilInputSlice.INSTANCE
        ),
        slicer.sliceStatic(spec, null, 3)
    );
  }

  @Test
  public void test_sliceDynamic_none()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2002/P1M")),
        null
    );

    Assertions.assertEquals(
        Collections.emptyList(),
        slicer.sliceDynamic(spec, null, 1, 1, 1)
    );
  }

  @Test
  public void test_sliceDynamic_maxOneSlice()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assertions.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceDynamic(spec, null, 1, 1, 1)
    );
  }

  @Test
  public void test_sliceDynamic_needOne()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assertions.assertEquals(
        Collections.singletonList(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT1.getVersion(),
                        SEGMENT1.getShardSpec().getPartitionNum()
                    ),
                    new RichSegmentDescriptor(
                        SEGMENT2.getInterval(),
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceDynamic(spec, null, 100, 5, BYTES_PER_SEGMENT * 5)
    );
  }

  @Test
  public void test_sliceDynamic_needTwoDueToFiles()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
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
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceDynamic(spec, null, 100, 1, BYTES_PER_SEGMENT * 5)
    );
  }

  @Test
  public void test_sliceDynamic_needTwoDueToBytes()
  {
    final TableInputSpec spec = new TableInputSpec(
        DATASOURCE,
        ImmutableList.of(Intervals.of("2000/P1M")),
        null
    );

    Assertions.assertEquals(
        ImmutableList.of(
            new SegmentsInputSlice(
                DATASOURCE,
                ImmutableList.of(
                    new RichSegmentDescriptor(
                        SEGMENT1.getInterval(),
                        Intervals.of("2000/P1M"),
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
                        Intervals.of("2000/P1M"),
                        SEGMENT2.getVersion(),
                        SEGMENT2.getShardSpec().getPartitionNum()
                    )
                ),
                ImmutableList.of()
            )
        ),
        slicer.sliceDynamic(spec, null, 100, 5, BYTES_PER_SEGMENT)
    );
  }
  @Test
  public void test_sliceStatic_clusterGroupsPruneSegments()
  {
    Assertions.assertEquals(
        ImmutableList.of(clusteredSliceOf(CLUSTERED_SEGMENT3)),
        slicer.sliceStatic(new TableInputSpec(CLUSTERED_DATASOURCE, null, null), excludeTenantPruner("acme"), 1)
    );
  }

  @Test
  public void test_sliceDynamic_clusterGroupsPruneSegments()
  {
    Assertions.assertEquals(
        ImmutableList.of(clusteredSliceOf(CLUSTERED_SEGMENT3)),
        slicer.sliceDynamic(
            new TableInputSpec(CLUSTERED_DATASOURCE, null, null),
            excludeTenantPruner("acme"),
            1,
            100,
            BYTES_PER_SEGMENT * 100
        )
    );
  }

  @Test
  public void test_sliceStatic_withoutPruner_requestsNoSegmentDetails()
  {
    slicer.sliceStatic(new TableInputSpec(CLUSTERED_DATASOURCE, null, null), null, 1);

    Assertions.assertEquals(SegmentDetail.none(), lastRetrieveUsedSegmentsAction.getDetails());
  }

  @Test
  public void test_sliceStatic_requestsSegmentDetailsDeclaredByPruner()
  {
    final SegmentPruner pruner = new SegmentPruner()
    {
      @Override
      public boolean include(DataSegment segment)
      {
        return true;
      }

      @Override
      public EnumSet<SegmentDetail> getRequiredSegmentDetails()
      {
        return EnumSet.of(SegmentDetail.DIMENSIONS);
      }

      @Override
      public SegmentPruner combine(SegmentPruner other)
      {
        throw new UnsupportedOperationException();
      }
    };

    slicer.sliceStatic(new TableInputSpec(CLUSTERED_DATASOURCE, null, null), pruner, 1);

    Assertions.assertEquals(EnumSet.of(SegmentDetail.DIMENSIONS), lastRetrieveUsedSegmentsAction.getDetails());
  }

  private static FilterSegmentPruner excludeTenantPruner(String tenant)
  {
    return new FilterSegmentPruner(
        new NotDimFilter(new EqualityFilter("tenant", ColumnType.STRING, tenant, null)),
        null,
        null
    );
  }

  private static SegmentsInputSlice clusteredSliceOf(DataSegment segment)
  {
    return new SegmentsInputSlice(
        CLUSTERED_DATASOURCE,
        ImmutableList.of(
            new RichSegmentDescriptor(
                segment.getInterval(),
                segment.getInterval(),
                segment.getVersion(),
                segment.getShardSpec().getPartitionNum()
            )
        ),
        ImmutableList.of()
    );
  }

  private static DataSegment makeClusteredSegment(
      int partitionNum,
      @Nullable StringTuple start,
      @Nullable StringTuple end,
      List<List<Object>> tuples
  )
  {
    return DataSegment.builder(SegmentId.of(CLUSTERED_DATASOURCE, Intervals.of("2002/2003"), "1", partitionNum))
                      .loadSpec(Collections.emptyMap())
                      .dimensions(Collections.emptyList())
                      .metrics(Collections.emptyList())
                      .shardSpec(new DimensionRangeShardSpec(
                          ImmutableList.of("tenant", "region"),
                          VirtualColumns.EMPTY,
                          start,
                          end,
                          partitionNum,
                          3
                      ))
                      .clusterGroups(new ClusterGroupTuples(CLUSTERING_COLUMNS, tuples))
                      .binaryVersion(null)
                      .size(BYTES_PER_SEGMENT)
                      .build();
  }
}
