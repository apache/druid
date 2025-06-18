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

package org.apache.druid.msq.kernel.controller;

import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.SlicerUtils;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.input.stage.StageInputSpecSlicer;
import org.apache.druid.msq.input.stage.StripedReadablePartitions;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.querykit.common.OffsetLimitStageProcessor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

public class WorkerInputsTest
{
  private static final String QUERY_ID = "myQuery";

  @Test
  public void test_max_threeInputs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(1, 2, 3))
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.MAX,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice(1)))
                    .put(1, Collections.singletonList(new TestInputSlice(2)))
                    .put(2, Collections.singletonList(new TestInputSlice(3)))
                    .put(3, Collections.singletonList(new TestInputSlice()))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_max_threeInputs_fourWorkers_withGaps()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(1, 2, 3))
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(new IntAVLTreeSet(new int[]{1, 3, 4, 5}), true),
        WorkerAssignmentStrategy.MAX,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(1, Collections.singletonList(new TestInputSlice(1)))
                    .put(3, Collections.singletonList(new TestInputSlice(2)))
                    .put(4, Collections.singletonList(new TestInputSlice(3)))
                    .put(5, Collections.singletonList(new TestInputSlice()))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_max_zeroInputs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec())
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.MAX,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice()))
                    .put(1, Collections.singletonList(new TestInputSlice()))
                    .put(2, Collections.singletonList(new TestInputSlice()))
                    .put(3, Collections.singletonList(new TestInputSlice()))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_zeroInputSpecs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs()
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(NilInputSlice.INSTANCE))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_zeroInputSlices_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec())
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(NilInputSlice.INSTANCE))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_zeroInputSlices_broadcast_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec())
                       .broadcastInputs(IntSet.of(0))
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice()))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_threeInputs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(1, 2, 3))
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice(1, 2, 3)))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_oneInputStageWithThreePartitionsAndTwoWorkers_fourWorkerMax()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(1)
                       .inputs(new StageInputSpec(0))
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        new Int2IntAVLTreeMap(ImmutableMap.of(0, 2)),
        new StageInputSpecSlicer(
            new Int2ObjectAVLTreeMap<>(ImmutableMap.of(0, ReadablePartitions.striped(0, 2, 3))),
            new Int2ObjectAVLTreeMap<>(ImmutableMap.of(0, OutputChannelMode.LOCAL_STORAGE))
        ),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(
                            new StageInputSlice(
                                0,
                                new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{0, 2})),
                                OutputChannelMode.LOCAL_STORAGE
                            )
                        )
                    )
                    .put(
                        1,
                        Collections.singletonList(
                            new StageInputSlice(
                                0,
                                new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{1})),
                                OutputChannelMode.LOCAL_STORAGE
                            )
                        )
                    )
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_oneInputStageWithThreePartitionsAndTwoWorkers_oneWorkerMax()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(1)
                       .inputs(new StageInputSpec(0))
                       .maxWorkerCount(1)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        new Int2IntAVLTreeMap(ImmutableMap.of(0, 2)),
        new StageInputSpecSlicer(
            new Int2ObjectAVLTreeMap<>(ImmutableMap.of(0, ReadablePartitions.striped(0, 2, 3))),
            new Int2ObjectAVLTreeMap<>(ImmutableMap.of(0, OutputChannelMode.LOCAL_STORAGE))
        ),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(
                            new StageInputSlice(
                                0,
                                new StripedReadablePartitions(0, 2, new IntAVLTreeSet(new int[]{0, 1, 2})),
                                OutputChannelMode.LOCAL_STORAGE
                            )
                        )
                    )
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_threeBigInputs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(200_000_000L, 200_000_001L, 200_000_002L))
                       .maxWorkerCount(4)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(4), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice(200_000_000L, 200_000_001L)))
                    .put(1, Collections.singletonList(new TestInputSlice(200_000_002L)))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_tenSmallAndOneBigInputs_twoWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(1, 2, 30_000_000_000L, 4, 5, 6, 7, 8, 9, 10, 11))
                       .maxWorkerCount(2)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(2), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(new TestInputSlice(1, 2, 4, 5, 6, 7, 8, 9, 10, 11))
                    )
                    .put(
                        1,
                        Collections.singletonList(new TestInputSlice(30_000_000_000L))
                    )
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_threeBigInputs_oneWorker()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(4_000_000_000L, 4_000_000_001L, 4_000_000_002L))
                       .maxWorkerCount(1)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(denseWorkers(1), true),
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(new TestInputSlice(4_000_000_000L, 4_000_000_001L, 4_000_000_002L))
                    )
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_max_shouldAlwaysSplitStatic()
  {
    TestInputSpec inputSpecToSplit = new TestInputSpec(4_000_000_000L);
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(inputSpecToSplit)
                       .maxWorkerCount(3)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    TestInputSpecSlicer testInputSpecSlicer = spy(new TestInputSpecSlicer(denseWorkers(3), true));

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        testInputSpecSlicer,
        WorkerAssignmentStrategy.MAX,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Mockito.verify(testInputSpecSlicer, times(0)).canSliceDynamic(inputSpecToSplit);
    Mockito.verify(testInputSpecSlicer, times(1)).sliceStatic(any(), anyInt());

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(new TestInputSlice(4_000_000_000L))
                    )
                    .put(
                        1,
                        Collections.singletonList(new TestInputSlice())
                    )
                    .put(
                        2,
                        Collections.singletonList(new TestInputSlice())
                    )
                    .build(),
        inputs.assignmentsMap()
    );

  }

  @Test
  public void test_auto_shouldSplitDynamicIfPossible()
  {
    TestInputSpec inputSpecToSplit = new TestInputSpec(1_000_000_000L, 1_000_000_000L, 1_000_000_000L);
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(inputSpecToSplit)
                       .maxWorkerCount(3)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    TestInputSpecSlicer testInputSpecSlicer = spy(new TestInputSpecSlicer(denseWorkers(3), true));

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        testInputSpecSlicer,
        WorkerAssignmentStrategy.AUTO,
        100
    );

    Mockito.verify(testInputSpecSlicer, times(1)).canSliceDynamic(inputSpecToSplit);
    Mockito.verify(testInputSpecSlicer, times(1)).sliceDynamic(any(), anyInt(), anyInt(), anyLong());

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(new TestInputSlice(1_000_000_000L))
                    )
                    .put(
                        1,
                        Collections.singletonList(new TestInputSlice(1_000_000_000L))
                    )
                    .put(
                        2,
                        Collections.singletonList(new TestInputSlice(1_000_000_000L))
                    )
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_shouldUseLeastWorkersPossible()
  {
    TestInputSpec inputSpecToSplit = new TestInputSpec(100_000_000L, 100_000_000L, 100_000_000L);
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(inputSpecToSplit)
                       .maxWorkerCount(3)
                       .processor(new OffsetLimitStageProcessor(0, 0L))
                       .build(QUERY_ID);

    TestInputSpecSlicer testInputSpecSlicer = spy(new TestInputSpecSlicer(denseWorkers(3), true));

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        testInputSpecSlicer,
        WorkerAssignmentStrategy.AUTO,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(
                        0,
                        Collections.singletonList(new TestInputSlice(100_000_000L, 100_000_000L, 100_000_000L))
                    )
                    .build(),
        inputs.assignmentsMap()
    );

    Mockito.verify(testInputSpecSlicer, times(1)).canSliceDynamic(inputSpecToSplit);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WorkerInputs.class).usingGetClass().verify();
  }

  private static class TestInputSpec implements InputSpec
  {
    private final LongList values;

    public TestInputSpec(long... values)
    {
      this.values = LongList.of(values);
    }
  }

  private static class TestInputSlice implements InputSlice
  {
    private final LongList values;

    public TestInputSlice(final LongList values)
    {
      this.values = values;
    }

    public TestInputSlice(long... values)
    {
      this.values = LongList.of(values);
    }

    @Override
    public int fileCount()
    {
      return values.size();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestInputSlice that = (TestInputSlice) o;
      return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(values);
    }

    @Override
    public String toString()
    {
      return values.toString();
    }
  }

  private static class TestInputSpecSlicer implements InputSpecSlicer
  {
    private final IntSortedSet workers;
    private final boolean canSliceDynamic;

    /**
     * Create a test slicer.
     *
     * @param workers         Set of workers to consider assigning work to.
     * @param canSliceDynamic Whether this slicer can slice dynamically.
     */
    public TestInputSpecSlicer(final IntSortedSet workers, final boolean canSliceDynamic)
    {
      this.workers = workers;
      this.canSliceDynamic = canSliceDynamic;

      if (workers.isEmpty()) {
        throw DruidException.defensive("Need more than one worker in workers[%s]", workers);
      }
    }

    @Override
    public boolean canSliceDynamic(InputSpec inputSpec)
    {
      return canSliceDynamic;
    }

    @Override
    public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
    {
      final TestInputSpec testInputSpec = (TestInputSpec) inputSpec;
      final List<List<Long>> assignments =
          SlicerUtils.makeSlicesStatic(
              testInputSpec.values.iterator(),
              i -> i,
              Math.min(maxNumSlices, workers.size())
          );
      return makeSlices(workers, assignments);
    }

    @Override
    public List<InputSlice> sliceDynamic(
        InputSpec inputSpec,
        int maxNumSlices,
        int maxFilesPerSlice,
        long maxBytesPerSlice
    )
    {
      final TestInputSpec testInputSpec = (TestInputSpec) inputSpec;
      final List<List<Long>> assignments =
          SlicerUtils.makeSlicesDynamic(
              testInputSpec.values.iterator(),
              i -> i,
              Math.min(maxNumSlices, workers.size()),
              maxFilesPerSlice,
              maxBytesPerSlice
          );
      return makeSlices(workers, assignments);
    }

    private static List<InputSlice> makeSlices(
        final IntSortedSet workers,
        final List<List<Long>> assignments
    )
    {
      final List<InputSlice> retVal = new ArrayList<>(assignments.size());
      for (int assignment = 0, workerNumber = 0;
           workerNumber <= workers.lastInt() && assignment < assignments.size();
           workerNumber++) {
        if (workers.contains(workerNumber)) {
          retVal.add(new TestInputSlice(new LongArrayList(assignments.get(assignment++))));
        } else {
          retVal.add(NilInputSlice.INSTANCE);
        }
      }

      return retVal;
    }
  }

  private static IntSortedSet denseWorkers(final int numWorkers)
  {
    final IntAVLTreeSet workers = new IntAVLTreeSet();
    for (int i = 0; i < numWorkers; i++) {
      workers.add(i);
    }
    return workers;
  }
}
