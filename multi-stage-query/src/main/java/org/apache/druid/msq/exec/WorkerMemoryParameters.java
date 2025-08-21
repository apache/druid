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

import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.SuperSorter;
import org.apache.druid.indexing.seekablestream.SeekableStreamAppenderatorConfig;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.indexing.processor.KeyStatisticsCollectionProcessor;
import org.apache.druid.msq.indexing.processor.SegmentGeneratorStageProcessor;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSlices;
import org.apache.druid.msq.input.stage.ReadablePartition;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.kernel.GlobalSortMaxCountShuffleSpec;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.querykit.BroadcastJoinSegmentMapFnProcessor;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.segment.incremental.IncrementalIndex;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Class for determining how much JVM heap to allocate to various purposes for executing a {@link WorkOrder}.
 *
 * First, we split each worker's memory allotment, given by {@link MemoryIntrospector#memoryPerTask()}, into
 * equally-sized "bundles" for each {@link WorkOrder} that may be running simultaneously within the {@link Worker}
 * for that {@link WorkOrder}.
 *
 * Within each bundle, we carve out memory required for buffering broadcast data
 * (see {@link #computeBroadcastBufferMemory}) and for concurrently-running processors
 * (see {@link #computeProcessorMemory}).
 *
 * The remainder is called "bundle free memory", a pool of memory that can be used for {@link SuperSorter} or
 * {@link SegmentGeneratorStageProcessor}. The amounts overlap, because the same {@link WorkOrder} never
 * does both.
 */
public class WorkerMemoryParameters
{
  /**
   * Default size for frames.
   */
  public static final int DEFAULT_FRAME_SIZE = 1_000_000;

  /**
   * Amount of extra memory available for each processing thread, beyond what is needed for input and output
   * channels. This memory is used for miscellaneous purposes within the various {@link FrameProcessor}.
   */
  private static final long EXTRA_MEMORY_PER_PROCESSOR = 25_000_000;

  /**
   * Percent of each bundle's free memory that we allocate to appenderators. It is less than 100% because appenderators
   * unfortunately have a variety of unaccounted-for memory usage.
   */
  private static final double APPENDERATOR_BUNDLE_FREE_MEMORY_FRACTION = 0.67;

  /**
   * Maximum percent of each bundle's free memory that will be used for maxRetainedBytes of
   * {@link ClusterByStatisticsCollectorImpl}.
   */
  private static final double PARTITION_STATS_MAX_BUNDLE_FREE_MEMORY_FRACTION = 0.1;

  /**
   * Maximum number of bytes from each bundle's free memory that we'll ever use for maxRetainedBytes of
   * {@link ClusterByStatisticsCollectorImpl}. Limits the value computed based on
   * {@link #PARTITION_STATS_MAX_BUNDLE_FREE_MEMORY_FRACTION}.
   */
  private static final long PARTITION_STATS_MAX_MEMORY_PER_BUNDLE = 300_000_000;

  /**
   * Minimum number of bytes from each bundle's free memory that we'll use for maxRetainedBytes of
   * {@link ClusterByStatisticsCollectorImpl}.
   */
  private static final long PARTITION_STATS_MIN_MEMORY_PER_BUNDLE = 10_000_000;

  /**
   * Fraction of each bundle's total memory that can be used to buffer broadcast inputs. This is used by
   * {@link BroadcastJoinSegmentMapFnProcessor} to limit how much joinable data is stored on-heap. This is carved
   * directly out of the total bundle memory, which makes its size more predictable and stable: it only depends on
   * the total JVM memory, the number of tasks per JVM, and the value of maxConcurrentStages for the query. This
   * stability is important, because if the broadcast buffer fills up, the query fails. So any time its size changes,
   * we risk queries failing that would formerly have succeeded.
   */
  private static final double BROADCAST_BUFFER_TOTAL_MEMORY_FRACTION = 0.2;

  /**
   * Multiplier to apply to {@link #BROADCAST_BUFFER_TOTAL_MEMORY_FRACTION} when determining how much free bundle
   * memory is left over. This fudge factor exists because {@link BroadcastJoinSegmentMapFnProcessor} applies data
   * size limits based on frame size, which we expect to expand somewhat in memory due to indexing structures in
   * {@link org.apache.druid.segment.join.table.FrameBasedIndexedTable}.
   */
  private static final double BROADCAST_BUFFER_OVERHEAD_RATIO = 1.5;

  /**
   * Amount of memory that can be used by
   * {@link org.apache.druid.msq.querykit.common.SortMergeJoinFrameProcessor} to buffer frames in its trackers.
   */
  private static final long SORT_MERGE_JOIN_MEMORY_PER_PROCESSOR = (long) (EXTRA_MEMORY_PER_PROCESSOR * 0.9);

  private final long bundleFreeMemory;
  private final int frameSize;
  private final int superSorterConcurrentProcessors;
  private final int superSorterMaxChannelsPerMerger;
  private final int partitionStatisticsMaxRetainedBytes;
  private final long broadcastBufferMemory;

  public WorkerMemoryParameters(
      final long bundleFreeMemory,
      final int frameSize,
      final int superSorterConcurrentProcessors,
      final int superSorterMaxChannelsPerMerger,
      final int partitionStatisticsMaxRetainedBytes,
      final long broadcastBufferMemory
  )
  {
    this.bundleFreeMemory = bundleFreeMemory;
    this.frameSize = frameSize;
    this.superSorterConcurrentProcessors = superSorterConcurrentProcessors;
    this.superSorterMaxChannelsPerMerger = superSorterMaxChannelsPerMerger;
    this.partitionStatisticsMaxRetainedBytes = partitionStatisticsMaxRetainedBytes;
    this.broadcastBufferMemory = broadcastBufferMemory;
  }

  /**
   * Create a production instance for a given {@link WorkOrder}.
   */
  public static WorkerMemoryParameters createProductionInstance(
      final WorkOrder workOrder,
      final MemoryIntrospector memoryIntrospector,
      final int maxConcurrentStages
  )
  {
    final StageDefinition stageDef = workOrder.getStageDefinition();
    return createInstance(
        memoryIntrospector,
        DEFAULT_FRAME_SIZE,
        workOrder.getInputs(),
        stageDef.getBroadcastInputNumbers(),
        stageDef.doesShuffle() ? stageDef.getShuffleSpec() : null,
        maxConcurrentStages,
        computeFramesPerOutputChannel(workOrder.getOutputChannelMode())
    );
  }

  /**
   * Returns an object specifying memory-usage parameters for a {@link WorkOrder} running inside a {@link Worker}.
   *
   * Throws a {@link MSQException} with an appropriate fault if the provided combination of parameters cannot
   * yield a workable memory situation.
   *
   * @param memoryIntrospector           memory introspector
   * @param frameSize                    frame size
   * @param inputSlices                  from {@link WorkOrder#getInputs()}
   * @param broadcastInputNumbers        from {@link StageDefinition#getBroadcastInputNumbers()}
   * @param shuffleSpec                  from {@link StageDefinition#getShuffleSpec()}
   * @param maxConcurrentStages          figure from {@link WorkerContext#maxConcurrentStages()}
   * @param numFramesPerOutputChannel    figure from {@link #computeFramesPerOutputChannel(OutputChannelMode)}
   *
   * @throws MSQException with {@link TooManyWorkersFault} or {@link NotEnoughMemoryFault} if not enough memory
   *                      is available to generate a usable instance
   */
  public static WorkerMemoryParameters createInstance(
      final MemoryIntrospector memoryIntrospector,
      final int frameSize,
      final List<InputSlice> inputSlices,
      final IntSet broadcastInputNumbers,
      @Nullable final ShuffleSpec shuffleSpec,
      final int maxConcurrentStages,
      final int numFramesPerOutputChannel
  )
  {
    final long bundleMemory = computeBundleMemory(memoryIntrospector.memoryPerTask(), maxConcurrentStages);
    final long processorMemory = computeProcessorMemory(
        computeMaxSimultaneousInputChannelsPerProcessor(inputSlices, broadcastInputNumbers),
        frameSize
    );
    final boolean hasBroadcastInputs = !broadcastInputNumbers.isEmpty();
    final long broadcastBufferMemory =
        hasBroadcastInputs ? computeBroadcastBufferMemoryIncludingOverhead(bundleMemory) : 0;
    final int numProcessingThreads = memoryIntrospector.numProcessingThreads();
    final int maxSimultaneousWorkProcessors = Math.min(numProcessingThreads, computeNumInputPartitions(inputSlices));
    final long bundleFreeMemory =
        bundleMemory - maxSimultaneousWorkProcessors * processorMemory - broadcastBufferMemory;

    final long minimumBundleFreeMemory = computeMinimumBundleFreeMemory(frameSize, numFramesPerOutputChannel);
    if (bundleFreeMemory < minimumBundleFreeMemory) {
      final long requiredTaskMemory = (bundleMemory - bundleFreeMemory + minimumBundleFreeMemory) * maxConcurrentStages;
      throw new MSQException(
          new NotEnoughMemoryFault(
              memoryIntrospector.computeJvmMemoryRequiredForTaskMemory(requiredTaskMemory),
              memoryIntrospector.totalMemoryInJvm(),
              memoryIntrospector.memoryPerTask(),
              memoryIntrospector.numTasksInJvm(),
              memoryIntrospector.numProcessingThreads(),
              computeNumInputWorkers(inputSlices),
              maxConcurrentStages
          )
      );
    }

    // Compute memory breakdown for super-sorting bundles.
    final int partitionStatsMemory =
        StageDefinition.mustGatherResultKeyStatistics(shuffleSpec) ? computePartitionStatsMemory(bundleFreeMemory) : 0;
    final long superSorterMemory = bundleFreeMemory - partitionStatsMemory;
    final int maxOutputPartitions = computeMaxOutputPartitions(shuffleSpec);

    int superSorterConcurrentProcessors;
    int superSorterMaxChannelsPerMerger = -1;

    if (maxOutputPartitions == 0) {
      superSorterConcurrentProcessors = numProcessingThreads;
    } else {
      superSorterConcurrentProcessors = Math.min(maxOutputPartitions, numProcessingThreads);
    }

    for (; superSorterConcurrentProcessors > 0; superSorterConcurrentProcessors--) {
      final long memoryPerProcessor = superSorterMemory / superSorterConcurrentProcessors;

      // Each processor has at least 2 frames for inputs, plus numFramesPerOutputChannel for outputs.
      // Compute whether we can support this level of parallelism, given these constraints.
      final int minMemoryForInputsPerProcessor = 2 * frameSize;
      final int memoryForOutputsPerProcessor = numFramesPerOutputChannel * frameSize;

      if (memoryPerProcessor >= minMemoryForInputsPerProcessor + memoryForOutputsPerProcessor) {
        final long memoryForInputsPerProcessor = memoryPerProcessor - memoryForOutputsPerProcessor;
        superSorterMaxChannelsPerMerger = Ints.checkedCast(memoryForInputsPerProcessor / frameSize);
        break;
      }
    }

    if (superSorterConcurrentProcessors == 0) {
      // Couldn't support any level of concurrency. Not expected, since we should have accounted for at least a
      // minimally-sized SuperSorter by way of the calculation in "computeMinimumBundleFreeMemory". Return a
      // NotEnoughMemoryFault with no suggestedServerMemory, since at this point, we aren't sure what will work.
      throw new MSQException(
          new NotEnoughMemoryFault(
              0,
              memoryIntrospector.totalMemoryInJvm(),
              memoryIntrospector.memoryPerTask(),
              memoryIntrospector.numTasksInJvm(),
              memoryIntrospector.numProcessingThreads(),
              computeNumInputWorkers(inputSlices),
              maxConcurrentStages
          )
      );
    }

    return new WorkerMemoryParameters(
        bundleFreeMemory,
        frameSize,
        superSorterConcurrentProcessors,
        superSorterMaxChannelsPerMerger,
        partitionStatsMemory,
        hasBroadcastInputs ? computeBroadcastBufferMemory(bundleMemory) : 0
    );
  }

  public int getSuperSorterConcurrentProcessors()
  {
    return superSorterConcurrentProcessors;
  }

  public int getSuperSorterMaxChannelsPerMerger()
  {
    return superSorterMaxChannelsPerMerger;
  }

  public long getAppenderatorMaxBytesInMemory()
  {
    // Half for indexing, half for merging.
    return Math.max(1, getAppenderatorMemory() / 2);
  }

  public int getAppenderatorMaxColumnsToMerge()
  {
    // Half for indexing, half for merging.
    final long calculatedMaxColumnsToMerge =
        getAppenderatorMemory() / 2 / SeekableStreamAppenderatorConfig.APPENDERATOR_MERGE_ROUGH_HEAP_MEMORY_PER_COLUMN;
    return Ints.checkedCast(Math.max(2, calculatedMaxColumnsToMerge));
  }

  public int getFrameSize()
  {
    return frameSize;
  }

  /**
   * Memory available for buffering broadcast data. Used to restrict the amount of memory used by
   * {@link BroadcastJoinSegmentMapFnProcessor}.
   */
  public long getBroadcastBufferMemory()
  {
    return broadcastBufferMemory;
  }

  /**
   * Fraction of each processor's memory that can be used by
   * {@link org.apache.druid.msq.querykit.common.SortMergeJoinFrameProcessor} to buffer frames in its trackers.
   */
  public long getSortMergeJoinMemory()
  {
    return SORT_MERGE_JOIN_MEMORY_PER_PROCESSOR;
  }

  public int getPartitionStatisticsMaxRetainedBytes()
  {
    return partitionStatisticsMaxRetainedBytes;
  }

  /**
   * Amount of memory to devote to {@link org.apache.druid.segment.realtime.appenderator.Appenderator}.
   */
  private long getAppenderatorMemory()
  {
    return (long) (bundleFreeMemory * APPENDERATOR_BUNDLE_FREE_MEMORY_FRACTION);
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
    WorkerMemoryParameters that = (WorkerMemoryParameters) o;
    return bundleFreeMemory == that.bundleFreeMemory
           && frameSize == that.frameSize
           && superSorterConcurrentProcessors == that.superSorterConcurrentProcessors
           && superSorterMaxChannelsPerMerger == that.superSorterMaxChannelsPerMerger
           && partitionStatisticsMaxRetainedBytes == that.partitionStatisticsMaxRetainedBytes
           && broadcastBufferMemory == that.broadcastBufferMemory;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        bundleFreeMemory,
        frameSize,
        superSorterConcurrentProcessors,
        superSorterMaxChannelsPerMerger,
        partitionStatisticsMaxRetainedBytes,
        broadcastBufferMemory
    );
  }

  @Override
  public String toString()
  {
    return "WorkerMemoryParameters{" +
           "bundleFreeMemory=" + bundleFreeMemory +
           ", frameSize=" + frameSize +
           ", superSorterConcurrentProcessors=" + superSorterConcurrentProcessors +
           ", superSorterMaxChannelsPerMerger=" + superSorterMaxChannelsPerMerger +
           ", partitionStatisticsMaxRetainedBytes=" + partitionStatisticsMaxRetainedBytes +
           ", broadcastBufferMemory=" + broadcastBufferMemory +
           '}';
  }

  /**
   * Compute the memory allocated to each {@link WorkOrder} within a {@link Worker}.
   */
  static long computeBundleMemory(final long memoryPerWorker, final int maxConcurrentStages)
  {
    return memoryPerWorker / maxConcurrentStages;
  }

  /**
   * Compute the memory allocated to {@link KeyStatisticsCollectionProcessor} within each bundle.
   */
  static int computePartitionStatsMemory(final long bundleFreeMemory)
  {
    return Ints.checkedCast(
        Math.max(
            (long) Math.min(
                bundleFreeMemory * PARTITION_STATS_MAX_BUNDLE_FREE_MEMORY_FRACTION,
                PARTITION_STATS_MAX_MEMORY_PER_BUNDLE
            ),
            PARTITION_STATS_MIN_MEMORY_PER_BUNDLE
        )
    );
  }

  /**
   * Compute the memory limit passed to {@link BroadcastJoinSegmentMapFnProcessor} within each worker bundle. This
   * is somewhat lower than {@link #computeBroadcastBufferMemoryIncludingOverhead}, because we expect some overhead on
   * top of this limit due to indexing structures. This overhead isn't accounted for by the processor
   * {@link BroadcastJoinSegmentMapFnProcessor} itself.
   */
  static long computeBroadcastBufferMemory(final long bundleMemory)
  {
    return (long) (bundleMemory * BROADCAST_BUFFER_TOTAL_MEMORY_FRACTION);
  }

  /**
   * Memory allocated to {@link BroadcastJoinSegmentMapFnProcessor} within each worker bundle, including
   * expected overhead.
   */
  static long computeBroadcastBufferMemoryIncludingOverhead(final long bundleMemory)
  {
    return (long) (computeBroadcastBufferMemory(bundleMemory) * BROADCAST_BUFFER_OVERHEAD_RATIO);
  }

  /**
   * Memory allocated to each processor within a bundle, including fixed overheads and buffered input and output frames.
   *
   * @param maxSimultaneousInputChannelsPerProcessor figure from {@link #computeMaxSimultaneousInputChannelsPerProcessor}
   * @param frameSize                                frame size
   */
  static long computeProcessorMemory(final int maxSimultaneousInputChannelsPerProcessor, final int frameSize)
  {
    return EXTRA_MEMORY_PER_PROCESSOR
           + computeProcessorMemoryForInputChannels(maxSimultaneousInputChannelsPerProcessor, frameSize)
           + frameSize /* output frame */;
  }

  /**
   * Memory allocated to each processor for reading its inputs.
   *
   * @param maxSimultaneousInputChannelsPerProcessor figure from {@link #computeMaxSimultaneousInputChannelsPerProcessor}
   * @param frameSize                                frame size
   */
  static long computeProcessorMemoryForInputChannels(
      final int maxSimultaneousInputChannelsPerProcessor,
      final int frameSize
  )
  {
    return (long) maxSimultaneousInputChannelsPerProcessor * frameSize;
  }

  /**
   * Number of input partitions across all {@link StageInputSlice}.
   */
  static int computeNumInputPartitions(final List<InputSlice> inputSlices)
  {
    int retVal = 0;

    for (final StageInputSlice slice : InputSlices.allStageSlices(inputSlices)) {
      retVal += Iterables.size(slice.getPartitions());
    }

    return retVal;
  }

  /**
   * Maximum number of input channels that a processor may have open at once, given the provided worker assignment.
   *
   * To compute this, we take the maximum number of workers associated with some partition for each slice. Then we sum
   * those maxes up for all broadcast slices, and for all non-broadcast slices, and take the max between those two.
   * The idea is that processors first read broadcast data, then read non-broadcast data, and during both phases
   * they should have at most one partition open from each slice at once.
   *
   * @param inputSlices           object from {@link WorkOrder#getInputs()}
   * @param broadcastInputNumbers object from {@link StageDefinition#getBroadcastInputNumbers()}
   */
  static int computeMaxSimultaneousInputChannelsPerProcessor(
      final List<InputSlice> inputSlices,
      final IntSet broadcastInputNumbers
  )
  {
    long totalNonBroadcastInputChannels = 0;
    long totalBroadcastInputChannels = 0;

    final List<StageInputSlice> allStageSlices = InputSlices.allStageSlices(inputSlices);

    for (int inputNumber = 0; inputNumber < allStageSlices.size(); inputNumber++) {
      final StageInputSlice slice = allStageSlices.get(inputNumber);

      int maxWorkers = 0;
      for (final ReadablePartition partition : slice.getPartitions()) {
        maxWorkers = Math.max(maxWorkers, partition.getWorkerNumbers().size());
      }

      if (broadcastInputNumbers.contains(inputNumber)) {
        totalBroadcastInputChannels += maxWorkers;
      } else {
        totalNonBroadcastInputChannels += maxWorkers;
      }
    }

    return Ints.checkedCast(Math.max(totalBroadcastInputChannels, totalNonBroadcastInputChannels));
  }


  /**
   * Distinct number of input workers.
   */
  static int computeNumInputWorkers(final List<InputSlice> inputSlices)
  {
    final IntSet workerNumbers = new IntOpenHashSet();

    for (final StageInputSlice slice : InputSlices.allStageSlices(inputSlices)) {
      for (final ReadablePartition partition : slice.getPartitions()) {
        workerNumbers.addAll(partition.getWorkerNumbers());
      }
    }

    return workerNumbers.size();
  }

  /**
   * Maximum number of output channels for a shuffle spec, or 0 if not knowable in advance.
   */
  static int computeMaxOutputPartitions(@Nullable final ShuffleSpec shuffleSpec)
  {
    if (shuffleSpec == null) {
      return 0;
    } else {
      switch (shuffleSpec.kind()) {
        case HASH:
        case HASH_LOCAL_SORT:
        case MIX:
          return shuffleSpec.partitionCount();

        case GLOBAL_SORT:
          if (shuffleSpec instanceof GlobalSortMaxCountShuffleSpec) {
            return ((GlobalSortMaxCountShuffleSpec) shuffleSpec).getMaxPartitions();
          }
          // Fall through

        default:
          return 0;
      }
    }
  }

  /**
   * Maximum number of output channels for a shuffle spec, or 0 if not knowable in advance.
   */
  static int computeFramesPerOutputChannel(final OutputChannelMode outputChannelMode)
  {
    // If durable storage is enabled, we need one extra frame per output channel.
    return outputChannelMode.isDurable() ? 2 : 1;
  }

  /**
   * Minimum number of bytes for a bundle's free memory allotment. This must be enough to reasonably produce and
   * persist an {@link IncrementalIndex}, or to run a {@link SuperSorter} with 1 thread and 2 frames.
   */
  static long computeMinimumBundleFreeMemory(final int frameSize, final int numFramesPerOutputChannel)
  {
    // Some for partition statistics.
    long minMemory = PARTITION_STATS_MIN_MEMORY_PER_BUNDLE;

    // Some for a minimally-sized super-sorter.
    minMemory += (long) (2 + numFramesPerOutputChannel) * frameSize;

    // That's enough. Don't consider the possibility that the bundle may be used for producing IncrementalIndex,
    // because PARTITION_STATS_MIN_MEMORY_PER_BUNDLE more or less covers that.
    return minMemory;
  }
}
