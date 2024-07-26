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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.google.inject.Injector;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.druid.msq.indexing.error.TooManyWorkersFault;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.BroadcastJoinSegmentMapFnProcessor;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollectorImpl;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;

import java.util.Objects;

/**
 * Class for determining how much JVM heap to allocate to various purposes.
 *
 * First, we take a chunk out of the total JVM heap that is dedicated for MSQ; see {@link #computeUsableMemoryInJvm}.
 *
 * Then, we carve out some space for each worker that may be running in our JVM; see {@link #memoryPerWorker}.
 *
 * Then, we split the rest into "bundles" of equal size; see {@link #memoryPerBundle}. The number of bundles is based
 * entirely on server configuration; this makes the calculation robust to different queries running simultaneously in
 * the same JVM.
 *
 * Within each bundle, we split up memory in two different ways: one assuming it'll be used for a
 * {@link org.apache.druid.frame.processor.SuperSorter}, and one assuming it'll be used for a regular
 * processor. Callers can then use whichever set of allocations makes sense. (We assume no single bundle
 * will be used for both purposes.)
 */
public class WorkerMemoryParameters
{
  private static final Logger log = new Logger(WorkerMemoryParameters.class);

  /**
   * Percent of memory that we allocate to bundles. It is less than 100% because we need to leave some space
   * left over for miscellaneous other stuff, and to ensure that GC pressure does not get too high.
   */
  static final double USABLE_MEMORY_FRACTION = 0.75;

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
   * Maximum percent of *total* available memory (not each bundle), i.e. {@link #USABLE_MEMORY_FRACTION}, that we'll
   * ever use for maxRetainedBytes of {@link ClusterByStatisticsCollectorImpl} across all workers.
   */
  private static final double PARTITION_STATS_MEMORY_MAX_FRACTION = 0.1;

  /**
   * Maximum number of bytes we'll ever use for maxRetainedBytes of {@link ClusterByStatisticsCollectorImpl} for
   * a single worker. Acts as a limit on the value computed based on {@link #PARTITION_STATS_MEMORY_MAX_FRACTION}.
   */
  private static final long PARTITION_STATS_MEMORY_MAX_BYTES = 300_000_000;

  /**
   * Threshold in bytes below which we assume that the worker is "small". While calculating the memory requirements for
   * a small worker, we try to be as conservatives with the estimates and the extra temporary space required by the
   * frames, since that can add up quickly and cause OOM.
   */
  private static final long SMALL_WORKER_CAPACITY_THRESHOLD_BYTES = 256_000_000;

  /**
   * Fraction of free memory per bundle that can be used by {@link BroadcastJoinSegmentMapFnProcessor} to store broadcast
   * data on-heap. This is used to limit the total size of input frames, which we expect to expand on-heap. Expansion
   * can potentially be somewhat over 2x: for example, strings are UTF-8 in frames, but are UTF-16 on-heap, which is
   * a 2x expansion, and object and index overhead must be considered on top of that. So we use a value somewhat
   * lower than 0.5.
   */
  static final double BROADCAST_JOIN_MEMORY_FRACTION = 0.3;

  /**
   * Fraction of free memory per bundle that can be used by
   * {@link org.apache.druid.msq.querykit.common.SortMergeJoinFrameProcessor} to buffer frames in its trackers.
   */
  static final double SORT_MERGE_JOIN_MEMORY_FRACTION = 0.9;

  /**
   * In case {@link NotEnoughMemoryFault} is thrown, a fixed estimation overhead is added when estimating total memory required for the process.
   */
  private static final long BUFFER_BYTES_FOR_ESTIMATION = 1000;

  private final long processorBundleMemory;
  private final int superSorterMaxActiveProcessors;
  private final int superSorterMaxChannelsPerProcessor;
  private final int partitionStatisticsMaxRetainedBytes;

  WorkerMemoryParameters(
      final long processorBundleMemory,
      final int superSorterMaxActiveProcessors,
      final int superSorterMaxChannelsPerProcessor,
      final int partitionStatisticsMaxRetainedBytes
  )
  {
    this.processorBundleMemory = processorBundleMemory;
    this.superSorterMaxActiveProcessors = superSorterMaxActiveProcessors;
    this.superSorterMaxChannelsPerProcessor = superSorterMaxChannelsPerProcessor;
    this.partitionStatisticsMaxRetainedBytes = partitionStatisticsMaxRetainedBytes;
  }

  /**
   * Create a production instance for {@link org.apache.druid.msq.indexing.MSQControllerTask}.
   */
  public static WorkerMemoryParameters createProductionInstanceForController(final Injector injector)
  {
    long totalLookupFootprint = computeTotalLookupFootprint(injector);
    return createInstance(
        Runtime.getRuntime().maxMemory(),
        computeNumWorkersInJvm(injector),
        computeNumProcessorsInJvm(injector),
        0,
        0,
        totalLookupFootprint
    );
  }

  /**
   * Create a production instance for {@link org.apache.druid.msq.indexing.MSQWorkerTask}.
   */
  public static WorkerMemoryParameters createProductionInstanceForWorker(
      final Injector injector,
      final QueryDefinition queryDef,
      final int stageNumber
  )
  {
    final StageDefinition stageDef = queryDef.getStageDefinition(stageNumber);
    final IntSet inputStageNumbers = InputSpecs.getStageNumbers(stageDef.getInputSpecs());
    final int numInputWorkers =
        inputStageNumbers.intStream()
                         .map(inputStageNumber -> queryDef.getStageDefinition(inputStageNumber).getMaxWorkerCount())
                         .sum();
    long totalLookupFootprint = computeTotalLookupFootprint(injector);

    final int numHashOutputPartitions;
    if (stageDef.doesShuffle() && stageDef.getShuffleSpec().kind().isHash()) {
      numHashOutputPartitions = stageDef.getShuffleSpec().partitionCount();
    } else {
      numHashOutputPartitions = 0;
    }

    return createInstance(
        Runtime.getRuntime().maxMemory(),
        computeNumWorkersInJvm(injector),
        computeNumProcessorsInJvm(injector),
        numInputWorkers,
        numHashOutputPartitions,
        totalLookupFootprint
    );
  }

  /**
   * Returns an object specifying memory-usage parameters.
   *
   * Throws a {@link MSQException} with an appropriate fault if the provided combination of parameters cannot
   * yield a workable memory situation.
   *
   * @param maxMemoryInJvm            memory available in the entire JVM. This will be divided amongst processors.
   * @param numWorkersInJvm           number of workers that can run concurrently in this JVM. Generally equal to
   *                                  the task capacity.
   * @param numProcessingThreadsInJvm size of the processing thread pool in the JVM.
   * @param numInputWorkers           total number of workers across all input stages.
   * @param numHashOutputPartitions   total number of output partitions, if using hash partitioning; zero if not using
   *                                  hash partitioning.
   * @param totalLookupFootprint      estimated size of the lookups loaded by the process.
   */
  public static WorkerMemoryParameters createInstance(
      final long maxMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numInputWorkers,
      final int numHashOutputPartitions,
      final long totalLookupFootprint
  )
  {
    Preconditions.checkArgument(maxMemoryInJvm > 0, "Max memory passed: [%s] should be > 0", maxMemoryInJvm);
    Preconditions.checkArgument(numWorkersInJvm > 0, "Number of workers: [%s] in jvm should be > 0", numWorkersInJvm);
    Preconditions.checkArgument(
        numProcessingThreadsInJvm > 0,
        "Number of processing threads [%s] should be > 0",
        numProcessingThreadsInJvm
    );
    Preconditions.checkArgument(numInputWorkers >= 0, "Number of input workers: [%s] should be >=0", numInputWorkers);
    Preconditions.checkArgument(
        totalLookupFootprint >= 0,
        "Lookup memory footprint: [%s] should be >= 0",
        totalLookupFootprint
    );
    final long usableMemoryInJvm = computeUsableMemoryInJvm(maxMemoryInJvm, totalLookupFootprint);
    final long workerMemory = memoryPerWorker(usableMemoryInJvm, numWorkersInJvm);
    final long bundleMemory = memoryPerBundle(usableMemoryInJvm, numWorkersInJvm, numProcessingThreadsInJvm);
    final long bundleMemoryForInputChannels = memoryNeededForInputChannels(numInputWorkers);
    final long bundleMemoryForHashPartitioning = memoryNeededForHashPartitioning(numHashOutputPartitions);
    final long bundleMemoryForProcessing =
        bundleMemory - bundleMemoryForInputChannels - bundleMemoryForHashPartitioning;

    if (bundleMemoryForProcessing < PROCESSING_MINIMUM_BYTES) {
      final int maxWorkers = computeMaxWorkers(
          usableMemoryInJvm,
          numWorkersInJvm,
          numProcessingThreadsInJvm,
          numHashOutputPartitions
      );

      if (maxWorkers > 0) {
        throw new MSQException(new TooManyWorkersFault(numInputWorkers, Math.min(Limits.MAX_WORKERS, maxWorkers)));
      } else {
        // Not enough memory for even one worker. More of a NotEnoughMemory situation than a TooManyWorkers situation.
        throw new MSQException(
            new NotEnoughMemoryFault(
                calculateSuggestedMinMemoryFromUsableMemory(
                    estimateUsableMemory(
                        numWorkersInJvm,
                        numProcessingThreadsInJvm,
                        PROCESSING_MINIMUM_BYTES + BUFFER_BYTES_FOR_ESTIMATION + bundleMemoryForInputChannels
                    ), totalLookupFootprint),
                maxMemoryInJvm,
                usableMemoryInJvm,
                numWorkersInJvm,
                numProcessingThreadsInJvm
            )
        );
      }
    }

    // Compute memory breakdown for super-sorting bundles.
    final int maxNumFramesForSuperSorter = Ints.checkedCast(bundleMemory / WorkerMemoryParameters.LARGE_FRAME_SIZE);

    if (maxNumFramesForSuperSorter < MIN_SUPER_SORTER_FRAMES) {
      throw new MSQException(
          new NotEnoughMemoryFault(
              calculateSuggestedMinMemoryFromUsableMemory(
                  estimateUsableMemory(
                      numWorkersInJvm,
                      (MIN_SUPER_SORTER_FRAMES + BUFFER_BYTES_FOR_ESTIMATION) * LARGE_FRAME_SIZE
                  ),
                  totalLookupFootprint
              ),
              maxMemoryInJvm,
              usableMemoryInJvm,
              numWorkersInJvm,
              numProcessingThreadsInJvm
          )
      );
    }

    final int superSorterMaxActiveProcessors = Math.min(
        numProcessingThreadsInJvm,
        Math.min(
            maxNumFramesForSuperSorter / MIN_SUPER_SORTER_FRAMES,
            MAX_SUPER_SORTER_PROCESSORS
        )
    );

    final int isSmallWorker = usableMemoryInJvm < SMALL_WORKER_CAPACITY_THRESHOLD_BYTES ? 1 : 0;
    // Apportion max frames to all processors equally, then subtract one to account for an output frame and one to account
    // for the durable storage's output frame in the supersorter. The extra frame is required in case of durable storage
    // since composing output channel factories keep a frame open while writing to them.
    // We only account for this extra frame in the workers where the heap size is relatively small to be more
    // conservative with the memory estimations. In workers with heap size larger than the frame size, we can get away
    // without accounting for this extra frame, and instead better parallelize the supersorter's operations.
    final int superSorterMaxChannelsPerProcessor = maxNumFramesForSuperSorter / superSorterMaxActiveProcessors
                                                   - 1
                                                   - isSmallWorker;
    if (superSorterMaxActiveProcessors <= 0) {
      throw new MSQException(
          new NotEnoughMemoryFault(
              calculateSuggestedMinMemoryFromUsableMemory(
                  estimateUsableMemory(
                      numWorkersInJvm,
                      numProcessingThreadsInJvm,
                      PROCESSING_MINIMUM_BYTES + BUFFER_BYTES_FOR_ESTIMATION + bundleMemoryForInputChannels
                  ), totalLookupFootprint),
              maxMemoryInJvm,
              usableMemoryInJvm,
              numWorkersInJvm,
              numProcessingThreadsInJvm
          )
      );
    }

    return new WorkerMemoryParameters(
        bundleMemoryForProcessing,
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        Ints.checkedCast(workerMemory) // 100% of worker memory is devoted to partition statistics
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
    return Math.max(1, getAppenderatorMemory() / 2);
  }

  public int getAppenderatorMaxColumnsToMerge()
  {
    // Half for indexing, half for merging.
    return Ints.checkedCast(Math.max(2, getAppenderatorMemory() / 2 / APPENDERATOR_MERGE_ROUGH_MEMORY_PER_COLUMN));
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
    return (long) (processorBundleMemory * BROADCAST_JOIN_MEMORY_FRACTION);
  }

  public long getSortMergeJoinMemory()
  {
    return (long) (processorBundleMemory * SORT_MERGE_JOIN_MEMORY_FRACTION);
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
    return (long) (processorBundleMemory * APPENDERATOR_MEMORY_FRACTION);
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
    return processorBundleMemory == that.processorBundleMemory
           && superSorterMaxActiveProcessors == that.superSorterMaxActiveProcessors
           && superSorterMaxChannelsPerProcessor == that.superSorterMaxChannelsPerProcessor
           && partitionStatisticsMaxRetainedBytes == that.partitionStatisticsMaxRetainedBytes;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        processorBundleMemory,
        superSorterMaxActiveProcessors,
        superSorterMaxChannelsPerProcessor,
        partitionStatisticsMaxRetainedBytes
    );
  }

  @Override
  public String toString()
  {
    return "WorkerMemoryParameters{" +
           "processorBundleMemory=" + processorBundleMemory +
           ", superSorterMaxActiveProcessors=" + superSorterMaxActiveProcessors +
           ", superSorterMaxChannelsPerProcessor=" + superSorterMaxChannelsPerProcessor +
           ", partitionStatisticsMaxRetainedBytes=" + partitionStatisticsMaxRetainedBytes +
           '}';
  }

  /**
   * Computes the highest value of numInputWorkers, for the given parameters, that can be passed to
   * {@link #createInstance} without resulting in a {@link TooManyWorkersFault}.
   *
   * Returns 0 if no number of workers would be OK.
   */
  static int computeMaxWorkers(
      final long usableMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final int numHashOutputPartitions
  )
  {
    final long bundleMemory = memoryPerBundle(usableMemoryInJvm, numWorkersInJvm, numProcessingThreadsInJvm);

    // Compute number of workers that gives us PROCESSING_MINIMUM_BYTES of memory per bundle, while accounting for
    // memoryNeededForInputChannels + memoryNeededForHashPartitioning.
    final int isHashing = numHashOutputPartitions > 0 ? 1 : 0;
    return Math.max(
        0,
        Ints.checkedCast((bundleMemory - PROCESSING_MINIMUM_BYTES) / ((long) STANDARD_FRAME_SIZE * (1 + isHashing)) - 1)
    );
  }

  /**
   * Computes the amount of memory needed to read a single partition from a given number of workers.
   */
  static long memoryNeededForInputChannels(final int numInputWorkers)
  {
    // Workers that read sorted inputs must open all channels at once to do an N-way merge. Calculate memory needs.
    // Requirement: one input frame per worker, one buffered output frame.
    return (long) STANDARD_FRAME_SIZE * (numInputWorkers + 1);
  }

  /**
   * Maximum number of workers that may exist in the current JVM.
   */
  private static int computeNumWorkersInJvm(final Injector injector)
  {
    final AppenderatorsManager appenderatorsManager = injector.getInstance(AppenderatorsManager.class);

    if (appenderatorsManager instanceof UnifiedIndexerAppenderatorsManager) {
      // CliIndexer
      return injector.getInstance(WorkerConfig.class).getCapacity();
    } else {
      // CliPeon
      return 1;
    }
  }

  /**
   * Maximum number of concurrent processors that exist in the current JVM.
   */
  private static int computeNumProcessorsInJvm(final Injector injector)
  {
    return injector.getInstance(Bouncer.class).getMaxCount();
  }

  /**
   * Compute the memory allocated to each worker. Includes anything that exists outside of processing bundles.
   *
   * Today, we only look at one thing: the amount of memory taken up by
   * {@link org.apache.druid.msq.statistics.ClusterByStatisticsCollector}. This is the single largest source of memory
   * usage outside processing bundles.
   */
  private static long memoryPerWorker(
      final long usableMemoryInJvm,
      final int numWorkersInJvm
  )
  {
    final long memoryForWorkers = (long) Math.min(
        usableMemoryInJvm * PARTITION_STATS_MEMORY_MAX_FRACTION,
        numWorkersInJvm * PARTITION_STATS_MEMORY_MAX_BYTES
    );

    return memoryForWorkers / numWorkersInJvm;
  }

  /**
   * Compute the memory allocated to each processing bundle. Any computation changes done to this method should also be done in its corresponding method {@link WorkerMemoryParameters#estimateUsableMemory(int, int, long)}
   */
  private static long memoryPerBundle(
      final long usableMemoryInJvm,
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm
  )
  {
    final int bundleCount = numWorkersInJvm + numProcessingThreadsInJvm;

    // Need to subtract memoryForWorkers off the top of usableMemoryInJvm, since this is reserved for
    // statistics collection.
    final long memoryForWorkers = numWorkersInJvm * memoryPerWorker(usableMemoryInJvm, numWorkersInJvm);
    final long memoryForBundles = usableMemoryInJvm - memoryForWorkers;

    // Divide up the usable memory per bundle.
    return memoryForBundles / bundleCount;
  }

  /**
   * Used for estimating the usable memory for better exception messages when {@link NotEnoughMemoryFault} is thrown.
   */
  private static long estimateUsableMemory(
      final int numWorkersInJvm,
      final int numProcessingThreadsInJvm,
      final long estimatedEachBundleMemory
  )
  {
    final int bundleCount = numWorkersInJvm + numProcessingThreadsInJvm;
    return estimateUsableMemory(numWorkersInJvm, estimatedEachBundleMemory * bundleCount);

  }

  /**
   * Add overheads to the estimated bundle memoery for all the workers. Checkout {@link WorkerMemoryParameters#memoryPerWorker(long, int)}
   * for the overhead calculation outside the processing bundles.
   */
  private static long estimateUsableMemory(final int numWorkersInJvm, final long estimatedTotalBundleMemory)
  {

    // Currently, we only add the partition stats overhead since it will be the single largest overhead per worker.
    final long estimateStatOverHeadPerWorker = PARTITION_STATS_MEMORY_MAX_BYTES;
    return estimatedTotalBundleMemory + (estimateStatOverHeadPerWorker * numWorkersInJvm);
  }

  private static long memoryNeededForHashPartitioning(final int numOutputPartitions)
  {
    // One standard frame for each processor output.
    // May be zero, since numOutputPartitions is zero if not using hash partitioning.
    return (long) STANDARD_FRAME_SIZE * numOutputPartitions;
  }

  /**
   * Amount of heap memory available for our usage. Any computation changes done to this method should also be done in
   * its corresponding method {@link WorkerMemoryParameters#calculateSuggestedMinMemoryFromUsableMemory}
   */
  private static long computeUsableMemoryInJvm(final long maxMemory, final long totalLookupFootprint)
  {
    // Always report at least one byte, to simplify the math in createInstance.
    return Math.max(
        1,
        (long) ((maxMemory - totalLookupFootprint) * USABLE_MEMORY_FRACTION)
    );
  }

  /**
   * Estimate amount of heap memory for the given workload to use in case usable memory is provided. This method is used
   * for better exception messages when {@link NotEnoughMemoryFault} is thrown.
   */
  private static long calculateSuggestedMinMemoryFromUsableMemory(long usuableMemeory, final long totalLookupFootprint)
  {
    return (long) ((usuableMemeory / USABLE_MEMORY_FRACTION) + totalLookupFootprint);
  }

  /**
   * Total estimated lookup footprint. Obtained by calling {@link LookupExtractor#estimateHeapFootprint()} on
   * all available lookups.
   */
  private static long computeTotalLookupFootprint(final Injector injector)
  {
    // Subtract memory taken up by lookups. Correctness of this operation depends on lookups being loaded *before*
    // we create this instance. Luckily, this is the typical mode of operation, since by default
    // druid.lookup.enableLookupSyncOnStartup = true.
    final LookupExtractorFactoryContainerProvider lookupManager =
        injector.getInstance(LookupExtractorFactoryContainerProvider.class);

    int lookupCount = 0;
    long lookupFootprint = 0;

    for (final String lookupName : lookupManager.getAllLookupNames()) {
      final LookupExtractorFactoryContainer container = lookupManager.get(lookupName).orElse(null);

      if (container != null) {
        try {
          final LookupExtractor extractor = container.getLookupExtractorFactory().get();
          lookupFootprint += extractor.estimateHeapFootprint();
          lookupCount++;
        }
        catch (Exception e) {
          log.noStackTrace().warn(e, "Failed to load lookup [%s] for size estimation. Skipping.", lookupName);
        }
      }
    }

    log.debug("Lookup footprint: %d lookups with %,d total bytes.", lookupCount, lookupFootprint);

    return lookupFootprint;
  }
}
