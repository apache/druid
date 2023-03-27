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
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.SlicerUtils;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.querykit.common.OffsetLimitFrameProcessorFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.MAX
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
  public void test_max_zeroInputs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec())
                       .maxWorkerCount(4)
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.MAX
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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice(1, 2, 3)))
                    .build(),
        inputs.assignmentsMap()
    );
  }

  @Test
  public void test_auto_threeBigInputs_fourWorkers()
  {
    final StageDefinition stageDef =
        StageDefinition.builder(0)
                       .inputs(new TestInputSpec(4_000_000_000L, 4_000_000_001L, 4_000_000_002L))
                       .maxWorkerCount(4)
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
    );

    Assert.assertEquals(
        ImmutableMap.<Integer, List<InputSlice>>builder()
                    .put(0, Collections.singletonList(new TestInputSlice(4_000_000_000L, 4_000_000_001L)))
                    .put(1, Collections.singletonList(new TestInputSlice(4_000_000_002L)))
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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
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
                       .processorFactory(new OffsetLimitFrameProcessorFactory(0, 0L))
                       .build(QUERY_ID);

    final WorkerInputs inputs = WorkerInputs.create(
        stageDef,
        Int2IntMaps.EMPTY_MAP,
        new TestInputSpecSlicer(true),
        WorkerAssignmentStrategy.AUTO
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
    private final boolean canSliceDynamic;

    public TestInputSpecSlicer(boolean canSliceDynamic)
    {
      this.canSliceDynamic = canSliceDynamic;
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
              maxNumSlices
          );
      return makeSlices(assignments);
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
              maxNumSlices,
              maxFilesPerSlice,
              maxBytesPerSlice
          );
      return makeSlices(assignments);
    }

    private static List<InputSlice> makeSlices(
        final List<List<Long>> assignments
    )
    {
      final List<InputSlice> retVal = new ArrayList<>(assignments.size());

      for (final List<Long> assignment : assignments) {
        retVal.add(new TestInputSlice(new LongArrayList(assignment)));
      }

      return retVal;
    }
  }
}
