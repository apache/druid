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

import com.google.common.primitives.Ints;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;

import java.util.Objects;

/**
 * Class for determining how much JVM heap to allocate to various purposes.
 *
 * First, we take {@link #USABLE_MEMORY_FRACTION} out of the total JVM heap and split it into "bundles" of
 * equal size. The number of bundles is based entirely on server configuration; this makes the calculation
 * robust to different queries running simultaneously in the same JVM.
 *
 * Then, we split up the resources for each bundle in two different ways: one assuming it'll be used for a
 * {@link org.apache.druid.frame.processor.SuperSorter}, and one assuming it'll be used for a regular
 * processor. Callers can then use whichever set of allocations makes sense. (We assume no single bundle
 * will be used for both purposes.)
 */
public class WorkerMemoryParameters
{
  /**
   * Percent of memory that we allocate to bundles. It is less than 100% because we need to leave some space
   * left over for miscellaneous other stuff, and to ensure that GC pressure does not get too high.
   */
  private static final double USABLE_MEMORY_FRACTION = 0.75;

  /**
   * Percent of each bundle's memory that we allocate to appenderators. It is less than 100% because appenderators
   * unfortunately have a variety of unaccounted-for memory usage.
   */
  static final double APPENDERATOR_MEMORY_FRACTION = 0.67;

  /**
   * Size for "standard frames", which are used for most purposes, except inputs to super-sorters.
   *
   * In particular, frames that travel between workers are always the minimum size. This is helpful because it makes
   * it easier to compute the amount of memory needed to merge input streams.
   */
  private static final int STANDARD_FRAME_SIZE = 1_000_000;

  /**
   * Size for "large frames", which are used for inputs and inner channels in to super-sorters.
   *
   * This is helpful because it minimizes the number of temporary files needed during super-sorting.
   */
  private static final int LARGE_FRAME_SIZE = 8_000_000;

  /**
   * Minimum amount of bundle memory available for processing (i.e., total bundle size minus the amount
   * needed for input channels). This memory is guaranteed to be available for things like segment generation
   * and broadcast data.
   */
  public static final long PROCESSING_MINIMUM_BYTES = 25_000_000;

  /**
   * Maximum amount of parallelism for the super-sorter. Higher amounts of concurrency tend to be wasteful.
   */
  private static final int MAX_SUPER_SORTER_PROCESSORS = 4;

  /**
   * Each super-sorter must have at least 1 processor with 2 input frames and 1 output frame. That's 3 total.
   */
  private static final int MIN_SUPER_SORTER_FRAMES = 3;

  /**
   * (Very) rough estimate of the on-heap overhead of reading a column.
   */
  private static final int APPENDERATOR_MERGE_ROUGH_MEMORY_PER_COLUMN = 3_000;

  /**
   * Fraction of free memory per bundle that can be used by {@link org.apache.druid.msq.querykit.BroadcastJoinHelper}
   * to store broadcast data on-heap. This is used to limit the total size of input frames, which we expect to
   * expand on-heap. Expansion can potentially be somewhat over 2x: for example, strings are UTF-8 in frames, but are
   * UTF-16 on-heap, which is a 2x expansion, and object and index overhead must be considered on top of that. So
   * we use a value somewhat lower than 0.5.
   */
  static final double BROADCAST_JOIN_MEMORY_FRACTION = 0.3;

  private final int superSorterMaxActiveProcessors;
  private final int superSorterMaxChannelsPerProcessor;
  private final long appenderatorMemory;
  private final long broadcastJoinMemory;

  WorkerMemoryParameters(
      final int superSorterMaxActiveProcessors,
      final int superSorterMaxChannelsPerProcessor,
      final long appenderatorMemory,
      final long broadcastJoinMemory
  )
  {
    this.superSorterMaxActiveProcessors = superSorterMaxActiveProcessors;
    this.superSorterMaxChannelsPerProcessor = superSorterMaxChannelsPerProcessor;
    this.appenderatorMemory = appenderatorMemory;
    this.broadcastJoinMemory = broadcastJoinMemory;
  }

  /**
   * Returns an object specifying memory-usage parameters for a stage in a worker.
   *
   * Throws a {@link MSQException} with an appropriate fault if the provided combination of parameters cannot
   * yield a workable memory situation.
   *
   * @param maxMemoryInJvm            memory available in the entire JVM. This will be divided amongst processors.
   * @param numWorkersInJvm           number of workers that can run concurrently in this JVM. Generally equal to
   *                                  the task capacity.
   * @param numProcessingThreadsInJvm size of the processing thread pool in the JVM.
   * @param numInputWorkers           number of workers across input stages that need to be merged together.
   */
  public static WorkerMemoryParameters compute(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numInputWorkers
  )
  {
    final long bundleMemory = memoryPerBundle(maxMemoryInJvm, numWorkersInJvm, numProcessingThreadsInJvm);
    final long bundleMemoryForInputChannels = memoryNeededForInputChannels(numInputWorkers);
    final long bundleMemoryForProcessing = bundleMemory - bundleMemoryForInputChannels;

    if (bundleMemoryForProcessing < PROCESSING_MINIMUM_BYTES) {
      final int maxWorkers = computeMaxWorkers(maxMemoryInJvm, numWorkersInJvm, numProcessingThreadsInJvm);

      if (maxWorkers > 0) {
        throw new MSQException(new TooManyWorkersFault(numInputWorkers, Math.min(Limits.MAX_WORKERS, maxWorkers)));
      } else {
        // Not enough memory for even one worker. More of a NotEnoughMemory situation than a TooManyWorkers situation.
        throw new MSQException(
            new NotEnoughMemoryFault(
                maxMemoryInJvm,
                numWorkersInJvm,
                numProcessingThreadsInJvm
            )
        );
      }
    }

    // Compute memory breakdown for super-sorting bundles.
    final int maxNumFramesForSuperSorter = Ints.checkedCast(bundleMemory / WorkerMemoryParameters.LARGE_FRAME_SIZE);

    if (maxNumFramesForSuperSorter < MIN_SUPER_SORTER_FRAMES) {
      throw new MSQException(new NotEnoughMemoryFault(maxMemoryInJvm, numWorkersInJvm, numProcessingThreadsInJvm));
    }

    final int superSorterMaxActiveProcessors = Math.min(
        numProcessingThreadsInJvm,
        Math.min(
            maxNumFramesForSuperSorter / MIN_SUPER_SORTER_FRAMES,
            MAX_SUPER_SORTER_PROCESSORS
        )
    );

    // Apportion max frames to all processors equally, then subtract one to account for an output frame.
    final int superSorterMaxChannelsPerProcessor = maxNumFramesForSuperSorter / superSorterMaxActiveProcessors - 1;

    return new WorkerMemoryParameters(
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        (long) (bundleMemoryForProcessing * APPENDERATOR_MEMORY_FRACTION),
        (long) (bundleMemoryForProcessing * BROADCAST_JOIN_MEMORY_FRACTION)
    );
  }

  public int getSuperSorterMaxActiveProcessors()
  {
    return superSorterMaxActiveProcessors;
  }

  public int getSuperSorterMaxChannelsPerProcessor()
  {
    return superSorterMaxChannelsPerProcessor;
  }

  public long getAppenderatorMaxBytesInMemory()
  {
    // Half for indexing, half for merging.
    return Math.max(1, appenderatorMemory / 2);
  }

  public int getAppenderatorMaxColumnsToMerge()
  {
    // Half for indexing, half for merging.
    return Ints.checkedCast(Math.max(2, appenderatorMemory / 2 / APPENDERATOR_MERGE_ROUGH_MEMORY_PER_COLUMN));
  }

  public int getStandardFrameSize()
  {
    return STANDARD_FRAME_SIZE;
  }

  public int getLargeFrameSize()
  {
    return LARGE_FRAME_SIZE;
  }

  public long getBroadcastJoinMemory()
  {
    return broadcastJoinMemory;
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
    return superSorterMaxActiveProcessors == that.superSorterMaxActiveProcessors
           && superSorterMaxChannelsPerProcessor == that.superSorterMaxChannelsPerProcessor
           && appenderatorMemory == that.appenderatorMemory
           && broadcastJoinMemory == that.broadcastJoinMemory;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        appenderatorMemory,
        broadcastJoinMemory
    );
  }

  @Override
  public String toString()
  {
    return "WorkerMemoryParameters{" +
           "superSorterMaxActiveProcessors=" + superSorterMaxActiveProcessors +
           ", superSorterMaxChannelsPerProcessor=" + superSorterMaxChannelsPerProcessor +
           ", appenderatorMemory=" + appenderatorMemory +
           ", broadcastJoinMemory=" + broadcastJoinMemory +
           '}';
  }

  /**
   * Computes the highest value of numInputWorkers, for the given parameters, that can be passed to
   * {@link #compute} without resulting in a {@link TooManyWorkersFault}.
   *
   * Returns 0 if no number of workers would be OK.
   */
  static int computeMaxWorkers(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm
  )
  {
    final long bundleMemory = memoryPerBundle(maxMemoryInJvm, numWorkersInJvm, numProcessingThreadsInJvm);

    // Inverse of memoryNeededForInputChannels.
    return Ints.checkedCast((bundleMemory - PROCESSING_MINIMUM_BYTES) / STANDARD_FRAME_SIZE - 1);
  }

  private static long memoryPerBundle(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm
  )
  {
    final int bundleCount = numWorkersInJvm + numProcessingThreadsInJvm;
    return (long) (maxMemoryInJvm * USABLE_MEMORY_FRACTION) / bundleCount;
  }

  private static long memoryNeededForInputChannels(final int numInputWorkers)
  {
    // Regular processors require input-channel-merging for their inputs. Calculate how much that is.
    // Requirement: inputChannelsPerProcessor number of input frames, one output frame.
    return (long) STANDARD_FRAME_SIZE * (numInputWorkers + 1);
  }
}
