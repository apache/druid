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

package org.apache.druid.msq.exec;

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.GlobalSortTargetSizeShuffleSpec;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class WorkerMemoryParametersTest
{
  @Test
  public void test_1WorkerInJvm_alone_1Thread()
  {
    final int numThreads = 1;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(973_000_000, frameSize, 1, 874, 97_300_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_withBroadcast_1Thread()
  {
    final int numThreads = 1;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(
        ReadablePartitions.striped(0, 1, numThreads),
        ReadablePartitions.striped(0, 1, 1)
    );
    final IntSet broadcastInputs = IntSets.singleton(1);
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(673_000_000, frameSize, 1, 604, 67_300_000, 200_000_000),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_4Threads()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(892_000_000, frameSize, 4, 199, 89_200_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_withBroadcast_4Threads()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(
        ReadablePartitions.striped(0, 1, numThreads),
        ReadablePartitions.striped(0, 1, 1)
    );
    final IntSet broadcastInputs = IntSets.singleton(1);
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(592_000_000, frameSize, 4, 132, 59_200_000, 200_000_000),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_noStats_4Threads()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, 4);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = null;

    Assert.assertEquals(
        new WorkerMemoryParameters(892_000_000, frameSize, 4, 222, 0, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_2ConcurrentStages_4Threads()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(392_000_000, frameSize, 4, 87, 39_200_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 2, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_2ConcurrentStages_4Threads_highHeap()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(6_250_000_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(2_392_000_000L, frameSize, 4, 537, 239_200_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 2, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_32Threads()
  {
    final int numThreads = 32;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(136_000_000, frameSize, 32, 2, 13_600_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_33Threads()
  {
    final int numThreads = 33;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(109_000_000, frameSize, 32, 2, 10_900_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_40Threads()
  {
    final int numThreads = 40;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> WorkerMemoryParameters.createInstance(
            memoryIntrospector,
            frameSize,
            slices,
            broadcastInputs,
            shuffleSpec,
            1,
            1
        )
    );

    Assert.assertEquals(
        new NotEnoughMemoryFault(1_366_250_000, 1_250_000_000, 1_000_000_000, 1, 40, 1, 1),
        e.getFault()
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_40Threads_slightlyLessMemoryThanError()
  {
    // Test with one byte less than the amount of memory recommended in the error message
    // for test_1WorkerInJvm_alone_40Threads.
    final int numThreads = 40;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_366_250_000 - 1, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> WorkerMemoryParameters.createInstance(
            memoryIntrospector,
            frameSize,
            slices,
            broadcastInputs,
            shuffleSpec,
            1,
            1
        )
    );

    Assert.assertEquals(
        new NotEnoughMemoryFault(1_366_250_000, 1_366_249_999, 1_092_999_999, 1, 40, 1, 1),
        e.getFault()
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_40Threads_memoryFromError()
  {
    // Test with the amount of memory recommended in the error message for test_1WorkerInJvm_alone_40Threads.
    final int numThreads = 40;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_366_250_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(13_000_000, frameSize, 1, 2, 10_000_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_40Threads_2ConcurrentStages()
  {
    final int numThreads = 40;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(1_250_000_000, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    final MSQException e = Assert.assertThrows(
        MSQException.class,
        () -> WorkerMemoryParameters.createInstance(
            memoryIntrospector,
            frameSize,
            slices,
            broadcastInputs,
            shuffleSpec,
            2,
            1
        )
    );

    Assert.assertEquals(
        new NotEnoughMemoryFault(2_732_500_000L, 1_250_000_000, 1_000_000_000, 1, 40, 1, 2),
        e.getFault()
    );
  }

  @Test
  public void test_1WorkerInJvm_alone_40Threads_2ConcurrentStages_memoryFromError()
  {
    // Test with the amount of memory recommended in the error message from
    // test_1WorkerInJvm_alone_40Threads_2ConcurrentStages.
    final int numThreads = 40;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(2_732_500_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 1, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(13_000_000, frameSize, 1, 2, 10_000_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 2, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_200WorkersInCluster_4Threads()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(2_500_000_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 200, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(1_096_000_000, frameSize, 4, 245, 109_600_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_200WorkersInCluster_4Threads_2OutputPartitions()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(2_500_000_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 200, 2));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(1_548_000_000, frameSize, 4, 347, 154_800_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_200WorkersInCluster_2ConcurrentStages_4Threads()
  {
    final int numThreads = 4;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(2_500_000_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 200, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(96_000_000, frameSize, 4, 20, 10_000_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 2, 1)
    );
  }

  @Test
  public void test_12WorkersInJvm_200WorkersInCluster_64Threads_4OutputPartitions()
  {
    final int numThreads = 64;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(40_000_000_000L, 12, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 200, 4));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(1_762_666_666, frameSize, 64, 23, 176_266_666, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void test_12WorkersInJvm_200WorkersInCluster_2ConcurrentStages_64Threads_4OutputPartitions()
  {
    final int numThreads = 64;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(40_000_000_000L, 12, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, 200, 4));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(429_333_333, frameSize, 64, 5, 42_933_333, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 2, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_MaxWorkersInCluster_2ConcurrentStages_2Threads()
  {
    final int numWorkers = Limits.MAX_WORKERS;
    final int numThreads = 2;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(6_250_000_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, numWorkers, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(448_000_000, frameSize, 2, 200, 44_800_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 2, 1)
    );
  }

  @Test
  public void test_1WorkerInJvm_MaxWorkersInCluster_1Thread()
  {
    final int numWorkers = Limits.MAX_WORKERS;
    final int numThreads = 1;
    final int frameSize = WorkerMemoryParameters.DEFAULT_FRAME_SIZE;

    final MemoryIntrospectorImpl memoryIntrospector = createMemoryIntrospector(2_500_000_000L, 1, numThreads);
    final List<InputSlice> slices = makeInputSlices(ReadablePartitions.striped(0, numWorkers, numThreads));
    final IntSet broadcastInputs = IntSets.emptySet();
    final ShuffleSpec shuffleSpec = makeSortShuffleSpec();

    Assert.assertEquals(
        new WorkerMemoryParameters(974_000_000, frameSize, 1, 875, 97_400_000, 0),
        WorkerMemoryParameters.createInstance(memoryIntrospector, frameSize, slices, broadcastInputs, shuffleSpec, 1, 1)
    );
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(WorkerMemoryParameters.class).usingGetClass().verify();
  }

  private static MemoryIntrospectorImpl createMemoryIntrospector(
      final long totalMemory,
      final int numTasksInJvm,
      final int numProcessingThreads
  )
  {
    return new MemoryIntrospectorImpl(totalMemory, 0.8, numTasksInJvm, numProcessingThreads, null);
  }

  private static List<InputSlice> makeInputSlices(final ReadablePartitions... partitionss)
  {
    return Arrays.stream(partitionss)
                 .map(partitions -> new StageInputSlice(0, partitions, OutputChannelMode.LOCAL_STORAGE))
                 .collect(Collectors.toList());
  }

  private static ShuffleSpec makeSortShuffleSpec()
  {
    return new GlobalSortTargetSizeShuffleSpec(
        new ClusterBy(ImmutableList.of(new KeyColumn("foo", KeyOrder.ASCENDING)), 0),
        1_000_000,
        false
    );
  }
}
