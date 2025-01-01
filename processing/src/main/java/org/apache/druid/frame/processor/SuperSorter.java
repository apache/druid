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

package org.apache.druid.frame.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.PartitionedReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.RoundingMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * Sorts and partitions a dataset using parallel, possibly-external merge sort.
 *
 * Input is provided as a set of {@link ReadableFrameChannel} and output is provided as {@link OutputChannels}.
 * Work is performed on a provided {@link FrameProcessorExecutor}.
 *
 * The most central point for SuperSorter logic is the {@link #runWorkersIfPossible} method, which determines what
 * needs to be done next based on the current state of the SuperSorter.
 *
 * First, input channels are read into {@link #inputBuffer} using {@link FrameChannelBatcher}, launched via
 * {@link #runNextBatcher()}.
 *
 * If the sorter finishes reading its input before reaching {@link #getMaxInputBufferFramesForDirectMerging()} number of
 * frames in the {@link #inputBuffer}, the sorter operates in "direct mode". In this mode, {@link #totalMergingLevels}
 * is 1. Mergers launched by {@link #runNextDirectMerger()} directly merge input frames into output channels, fully
 * in-memory. No temporary files are used. Each direct merger reads *all* frames, but only rows corresponding to a
 * single output partition.
 *
 * Otherwise, the sorter operates in "external mode". In this mode, {@link #totalMergingLevels} is 2 or more. Logic
 * for external mode is:
 *
 * 1) Read inputs into {@link #inputBuffer} using {@link FrameChannelBatcher} (same as direct mode).
 *
 * 2) Merge and write frames from {@link #inputBuffer} into {@link FrameFile} scratch files using
 * {@link FrameChannelMerger} launched via {@link #runNextLevelZeroMerger()}.
 *
 * 3a) Merge level 0 scratch files into level 1 scratch files using {@link FrameChannelMerger} launched from
 * {@link #runNextMiddleMerger()}, processing up to {@link #maxChannelsPerMerger} files per merger.
 * Continue this process through increasing level numbers, with the size of scratch files increasing by a factor
 * of {@link #maxChannelsPerMerger} each level.
 *
 * 3b) For the penultimate level, the {@link FrameChannelMerger} launched by {@link #runNextMiddleMerger()} writes
 * partitioned {@link FrameFile} scratch files. The penultimate level cannot be written until
 * {@link #outputPartitionsFuture} resolves, so if it has not resolved yet by this point, the SuperSorter pauses.
 * The SuperSorter resumes and writes the penultimate level's files when the future resolves.
 *
 * 4) Write the final level using {@link FrameChannelMerger} launched from {@link #runNextUltimateMerger()}.
 * Outputs for this level are written to channels provided by {@link #outputChannelFactory}, rather than scratch files.
 *
 * At all points, higher level processing is preferred over lower-level processing. Writing to final output files
 * is preferred over intermediate, and writing to intermediate files is preferred over reading inputs. These
 * preferences ensure that the amount of data buffered up in memory does not grow too large.
 *
 * Potential future work (things we could optimize if necessary):
 *
 * - Identify sorted runs of frames in the {@link #inputBuffer}, group them into smaller sets of channels.
 * - Combine (for example: aggregate) while merging.
 */
public class SuperSorter
{
  private static final Logger log = new Logger(SuperSorter.class);

  public static final long UNLIMITED = -1;
  public static final int UNKNOWN_LEVEL = -1;
  public static final long UNKNOWN_TOTAL = -1;

  private final List<ReadableFrameChannel> inputChannels;
  private final FrameReader frameReader;
  private final List<KeyColumn> sortKey;
  private final ListenableFuture<ClusterByPartitions> outputPartitionsFuture;
  private final FrameProcessorExecutor exec;
  private final FrameProcessorDecorator processorDecorator;
  private final OutputChannelFactory outputChannelFactory;
  private final OutputChannelFactory intermediateOutputChannelFactory;
  private final int maxChannelsPerMerger;
  private final int maxActiveProcessors;
  private final String cancellationId;
  private final boolean removeNullBytes;
  private final Object runWorkersLock = new Object();

  @GuardedBy("runWorkersLock")
  private boolean batcherIsRunning = false;

  @GuardedBy("runWorkersLock")
  private IntSet inputChannelsToRead = new IntOpenHashSet();

  @GuardedBy("runWorkersLock")
  private final Int2ObjectMap<LongSortedSet> outputsReadyByLevel = new Int2ObjectArrayMap<>();

  @GuardedBy("runWorkersLock")
  private List<OutputChannel> outputChannels = null;

  @GuardedBy("runWorkersLock")
  private final Map<String, PartitionedOutputChannel> levelAndRankToReadableChannelMap = new HashMap<>();

  @GuardedBy("runWorkersLock")
  private int activeProcessors = 0;

  @GuardedBy("runWorkersLock")
  private long totalInputFrames = UNKNOWN_TOTAL;

  @GuardedBy("runWorkersLock")
  private int totalMergingLevels = UNKNOWN_LEVEL;

  @GuardedBy("runWorkersLock")
  private final Queue<Frame> inputBuffer = new ArrayDeque<>();

  @GuardedBy("runWorkersLock")
  private long inputFramesReadSoFar = 0;

  @GuardedBy("runWorkersLock")
  private long levelZeroMergersRunSoFar = 0;

  @GuardedBy("runWorkersLock")
  private int ultimateMergersRunSoFar = 0;

  @GuardedBy("runWorkersLock")
  private SettableFuture<OutputChannels> allDone = null;

  @GuardedBy("runWorkersLock")
  SuperSorterProgressTracker superSorterProgressTracker;

  @GuardedBy("runWorkersLock")
  private long rowLimit;

  /**
   * See {@link #setNoWorkRunnable}.
   */
  @GuardedBy("runWorkersLock")
  private Runnable noWorkRunnable = null;

  /**
   * Initializes a SuperSorter.
   *
   * @param inputChannels                    input channels. All frames in these channels must be sorted according to the
   *                                         {@link ClusterBy#getColumns()}, or else sorting will not produce correct
   *                                         output.
   * @param frameReader                      frame reader for the input channels
   * @param sortKey                          desired sorting order
   * @param outputPartitionsFuture           a future that resolves to the desired output partitions. Sorting will block
   *                                         prior to writing out final outputs until this future resolves. However, the
   *                                         sorter will be able to read all inputs even if this future is unresolved.
   *                                         If output need not be partitioned, use
   *                                         {@link ClusterByPartitions#oneUniversalPartition()}. In this case a single
   *                                         sorted channel is generated.
   * @param exec                             executor to perform work in
   * @param outputChannelFactory             factory for partitioned, sorted output channels
   * @param intermediateOutputChannelFactory factory for intermediate data produced by sorting levels
   * @param maxActiveProcessors              maximum number of merging processors to execute at once in the provided
   *                                         {@link FrameProcessorExecutor}
   * @param maxChannelsPerMerger             maximum number of channels to merge at once, for regular mergers
   *                                         (does not apply to direct mergers; see
   *                                         {@link #getMaxInputBufferFramesForDirectMerging()})
   * @param rowLimit                         limit to apply during sorting. The limit is applied across all partitions,
   *                                         not to each partition individually. Use {@link #UNLIMITED} if there is
   *                                         no limit.
   * @param cancellationId                   cancellation id to use when running processors in the provided
   *                                         {@link FrameProcessorExecutor}.
   * @param superSorterProgressTracker       progress tracker
   */
  public SuperSorter(
      final List<ReadableFrameChannel> inputChannels,
      final FrameReader frameReader,
      final List<KeyColumn> sortKey,
      final ListenableFuture<ClusterByPartitions> outputPartitionsFuture,
      final FrameProcessorExecutor exec,
      final FrameProcessorDecorator processorDecorator,
      final OutputChannelFactory outputChannelFactory,
      final OutputChannelFactory intermediateOutputChannelFactory,
      final int maxActiveProcessors,
      final int maxChannelsPerMerger,
      final long rowLimit,
      @Nullable final String cancellationId,
      final SuperSorterProgressTracker superSorterProgressTracker,
      final boolean removeNullBytes
  )
  {
    this.inputChannels = inputChannels;
    this.frameReader = frameReader;
    this.sortKey = sortKey;
    this.outputPartitionsFuture = outputPartitionsFuture;
    this.exec = exec;
    this.processorDecorator = processorDecorator;
    this.outputChannelFactory = outputChannelFactory;
    this.intermediateOutputChannelFactory = intermediateOutputChannelFactory;
    this.maxChannelsPerMerger = maxChannelsPerMerger;
    this.maxActiveProcessors = maxActiveProcessors;
    this.rowLimit = rowLimit;
    this.cancellationId = cancellationId;
    this.superSorterProgressTracker = superSorterProgressTracker;
    this.removeNullBytes = removeNullBytes;

    for (int i = 0; i < inputChannels.size(); i++) {
      inputChannelsToRead.add(i);
    }

    if (maxActiveProcessors < 1) {
      throw new IAE("maxActiveProcessors[%d] < 1", maxActiveProcessors);
    }

    if (maxChannelsPerMerger < 2) {
      throw new IAE("maxChannelsPerMerger[%d] < 2", maxChannelsPerMerger);
    }

    if (rowLimit != UNLIMITED && rowLimit <= 0) {
      throw new IAE("rowLimit[%d] must be positive", rowLimit);
    }
  }

  /**
   * Starts sorting. Can only be called once. Work is performed in the {@link FrameProcessorExecutor} that was
   * passed to the constructor.
   *
   * Returns a future containing partitioned sorted output channels.
   */
  public ListenableFuture<OutputChannels> run()
  {
    synchronized (runWorkersLock) {
      if (allDone != null) {
        throw new ISE("Cannot run() more than once.");
      }

      allDone = SettableFuture.create();
      runWorkersIfPossible();

      // When output partitions become known, that may unblock some additional layers of merging.
      outputPartitionsFuture.addListener(
          () -> {
            synchronized (runWorkersLock) {
              if (outputPartitionsFuture.isDone()) { // Update the progress tracker
                superSorterProgressTracker.setTotalMergersForUltimateLevel(getOutputPartitions().size());
              }
              runWorkersIfPossible();
              setAllDoneIfPossible();
            }
          },
          exec.asExecutor(cancellationId)
      );

      return FutureUtils.futureWithBaggage(
          allDone,
          () -> {
            synchronized (runWorkersLock) {
              if (activeProcessors == 0) {
                cleanUp();
              }
            }
          }
      );
    }
  }

  /**
   * Sets a callback that enables tests to see when this SuperSorter cannot do any work. Only used for testing.
   */
  @VisibleForTesting
  void setNoWorkRunnable(final Runnable runnable)
  {
    synchronized (runWorkersLock) {
      this.noWorkRunnable = runnable;
    }
  }

  /**
   * Called when a worker finishes.
   */
  @GuardedBy("runWorkersLock")
  private void workerFinished()
  {
    activeProcessors -= 1;

    if (log.isDebugEnabled()) {
      log.debug(stateString());
    }

    runWorkersIfPossible();
    setAllDoneIfPossible();

    if (isAllDone() && activeProcessors == 0) {
      cleanUp();
    }
  }

  /**
   * Tries to launch a new worker, and returns whether it was doable.
   *
   * Later workers have priority, i.e., those responsible for merging higher levels of the merge tree. Workers that
   * read the original input channels have the lowest priority. This priority order ensures that we don't build up
   * too much unmerged data.
   */
  @GuardedBy("runWorkersLock")
  private void runWorkersIfPossible()
  {
    if (isAllDone()) {
      // Do nothing if the instance is all done. This can happen in case of error or cancellation.
      return;
    }

    setTotalMergingLevelsIfPossible();

    try {
      // Attempt to run mergers in reverse order, so we merge later layers first.
      while (activeProcessors < maxActiveProcessors
             && (runNextDirectMerger()
                 || runNextUltimateMerger()
                 || runNextMiddleMerger()
                 || runNextLevelZeroMerger()
                 || runNextBatcher())) {
        activeProcessors += 1;

        if (log.isDebugEnabled()) {
          log.debug(stateString());
        }
      }

      if (activeProcessors == 0 && noWorkRunnable != null) {
        log.debug("No active workers and no work left to start.");

        // Only called in tests. No need to bother with try/catch and such.
        noWorkRunnable.run();
      }
    }
    catch (Throwable e) {
      allDone.setException(e);
    }
  }

  @GuardedBy("runWorkersLock")
  private void setAllDoneIfPossible()
  {
    if (isAllDone()) {
      // Already done, no need to set allDone again.
      return;
    }

    try {
      if (totalInputFrames == 0 && outputPartitionsFuture.isDone()) {
        // No input data -- generate empty output channels.
        final ClusterByPartitions partitions = getOutputPartitions();
        final List<OutputChannel> channels = new ArrayList<>(partitions.size());

        for (int partitionNum = 0; partitionNum < partitions.size(); partitionNum++) {
          channels.add(outputChannelFactory.openNilChannel(partitionNum));
        }

        // OK to use wrap, not wrapReadOnly, because nil channels are already read-only.
        allDone.set(OutputChannels.wrap(channels));
      } else if (rowLimit == 0 && activeProcessors == 0) {
        // We had a row limit, and got it all the way down to zero.
        // Generate empty output channels for any partitions that we haven't written yet.
        for (int partitionNum = 0; partitionNum < outputChannels.size(); partitionNum++) {
          if (outputChannels.get(partitionNum) == null) {
            outputChannels.set(partitionNum, outputChannelFactory.openNilChannel(partitionNum));
            superSorterProgressTracker.addMergedBatchesForLevel(totalMergingLevels - 1, 1);
          }
        }

        // OK to use wrap, not wrapReadOnly, because all channels in this list are already read-only.
        allDone.set(OutputChannels.wrap(outputChannels));
      } else if (totalMergingLevels != UNKNOWN_LEVEL
                 && outputsReadyByLevel.containsKey(totalMergingLevels - 1)
                 && (outputsReadyByLevel.get(totalMergingLevels - 1).size() ==
                     getTotalMergersInLevel(totalMergingLevels - 1))) {
        // We're done!!
        // OK to use wrap, not wrapReadOnly, because all channels in this list are already read-only.
        allDone.set(OutputChannels.wrap(outputChannels));
      }
    }
    catch (Throwable e) {
      allDone.setException(e);
    }
  }

  @GuardedBy("runWorkersLock")
  private boolean runNextBatcher()
  {
    if (batcherIsRunning || inputChannelsToRead.isEmpty()) {
      return false;
    } else {
      batcherIsRunning = true;

      runWorker(
          new FrameChannelBatcher(inputChannels, maxChannelsPerMerger),
          result -> {
            final List<Frame> batch = result.lhs;
            final IntSet keepReading = result.rhs;

            synchronized (runWorkersLock) {
              inputBuffer.addAll(batch);
              inputFramesReadSoFar += batch.size();
              inputChannelsToRead = keepReading;

              if (inputChannelsToRead.isEmpty()) {
                inputChannels.forEach(ReadableFrameChannel::close);
                setTotalInputFrames(inputFramesReadSoFar);
                setTotalMergingLevelsIfPossible();
                runWorkersIfPossible();
              } else if (inputBuffer.size() >= maxChannelsPerMerger) {
                runWorkersIfPossible();
              }

              batcherIsRunning = false;
            }
          }
      );

      return true;
    }
  }

  /**
   * Launch a merger that reads from {@link #inputBuffer} and directly writes an output channel. This method only
   * does anything when the sorter is in "direct mode", i.e. when all input fits in memory. See class-level javadoc
   * for more details.
   */
  @GuardedBy("runWorkersLock")
  private boolean runNextDirectMerger()
  {
    // Direct merging is a single-level merge (totalMergingLevels == 1).
    if (!(totalMergingLevels == 1
          && allInputRead()
          && ultimateMergersRunSoFar < getTotalMergersInLevel(0))) {
      return false;
    }

    if (isLimited() && (rowLimit == 0 || activeProcessors > 0)) {
      // Run final-layer mergers one at a time, to ensure limit is applied across the entire dataset.
      return false;
    }

    final List<ReadableFrameChannel> in = new ArrayList<>();

    for (final Frame frame : inputBuffer) {
      in.add(singleReadableFrameChannel(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION)));
    }

    runMerger(0, ultimateMergersRunSoFar++, in, Collections.emptyList());
    return true;
  }

  /**
   * Level zero mergers read batches of frames from the "inputBuffer". These frames are individually sorted, but there
   * is no ordering between the frames. Their output is a sorted sequence of frames.
   */
  @GuardedBy("runWorkersLock")
  private boolean runNextLevelZeroMerger()
  {
    if (totalMergingLevels == 1) {
      // When there's just one merging level, the inputBuffer is merged by runNextDirectMerger, not this method.
      return false;
    }

    if (inputBuffer.isEmpty()) {
      // Nothing to merge.
      return false;
    }

    if (totalMergingLevels == UNKNOWN_LEVEL && inputBuffer.size() < getMaxInputBufferFramesForDirectMerging()) {
      // Buffer up as much as possible until we've determined the merging structure, or until the buffer is full.
      return false;
    }

    final List<ReadableFrameChannel> in = new ArrayList<>();

    while (in.size() < maxChannelsPerMerger) {
      final Frame frame = inputBuffer.poll();

      if (frame == null) {
        break;
      }

      in.add(singleReadableFrameChannel(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION)));
    }

    runMerger(0, levelZeroMergersRunSoFar++, in, ImmutableList.of());
    return true;
  }

  @GuardedBy("runWorkersLock")
  private boolean runNextMiddleMerger()
  {
    for (int inLevel = outputsReadyByLevel.size() - 1; inLevel >= 0; inLevel--) {
      final int outLevel = inLevel + 1;
      final long totalInputs = getTotalMergersInLevel(inLevel);
      final LongSortedSet inputsReady = outputsReadyByLevel.get(inLevel);

      if (totalMergingLevels != UNKNOWN_LEVEL && outLevel >= totalMergingLevels - 1) {
        // This is the ultimate level. Skip it, since it will be launched by either runNextDirectMerger (if one
        // merging level) or runNextUltimateMerger (if more than one merging level).
        continue;
      }

      if (totalMergingLevels == UNKNOWN_LEVEL
          && LongMath.divide(inputsReady.size(), maxChannelsPerMerger, RoundingMode.CEILING) <= maxChannelsPerMerger) {
        // This *might* be the penultimate level. Skip until we know for sure. (i.e., until all input frames have
        // been read.)
        continue;
      }

      final int channelsPerMerger;

      if (totalMergingLevels != UNKNOWN_LEVEL && outLevel == totalMergingLevels - 2) {
        // Definitely the penultimate level. Channels per merger may be less than maxChannelsPerMerger.
        if (outputPartitionsFuture.isDone()) {
          channelsPerMerger = Ints.checkedCast(
              LongMath.divide(
                  totalInputs,
                  getTotalMergersInLevel(outLevel),
                  RoundingMode.CEILING
              )
          );
        } else {
          // Can't run penultimate level until output partitions are known.
          continue;
        }
      } else {
        channelsPerMerger = maxChannelsPerMerger;
      }

      // See if there's work to do.

      final LongIterator iter = inputsReady.iterator();

      long currentSetStart = -1, currentSetIndex = -1;
      while (iter.hasNext()) {
        final long w = iter.nextLong();
        if (w % channelsPerMerger == 0) {
          // w is the start of a set
          currentSetStart = w;
          currentSetIndex = -1;
        }

        if (currentSetStart >= 0) {
          // We're currently exploring a potential set.
          long pos = w - currentSetStart;

          if (pos == currentSetIndex + 1 &&
              (pos == channelsPerMerger - 1 || (totalInputs != UNKNOWN_TOTAL && w == totalInputs - 1))) {
            // We found a set to merge. Let's collect the input channels and launch the merger.
            final List<ReadableFrameChannel> in = new ArrayList<>();
            final List<PartitionedReadableFrameChannel> partitionedReadableFrameChannels = new ArrayList<>();
            for (long i = currentSetStart; i < currentSetStart + channelsPerMerger; i++) {
              if (inputsReady.remove(i)) {
                String levelAndRankKey = mergerOutputFileName(inLevel, i);
                PartitionedReadableFrameChannel partitionedReadableFrameChannel =
                    levelAndRankToReadableChannelMap.remove(levelAndRankKey)
                                                    .getReadableChannelSupplier()
                                                    .get();
                in.add(partitionedReadableFrameChannel.getReadableFrameChannel(0));
                partitionedReadableFrameChannels.add(partitionedReadableFrameChannel);
              }
            }

            runMerger(
                outLevel,
                currentSetStart / channelsPerMerger,
                in,
                partitionedReadableFrameChannels
            );
            return true;
          } else if (w == currentSetStart + currentSetIndex + 1) {
            currentSetIndex++;
          } else {
            currentSetStart = -1;
            currentSetIndex = -1;
          }
        }
      }
    }

    // Nothing to merge (yet?).
    return false;
  }

  @GuardedBy("runWorkersLock")
  private boolean runNextUltimateMerger()
  {
    if (!(totalMergingLevels != UNKNOWN_LEVEL
          && totalMergingLevels >= 2
          && outputPartitionsFuture.isDone()
          && ultimateMergersRunSoFar < getOutputPartitions().size())) {
      return false;
    }

    if (isLimited() && (rowLimit == 0 || activeProcessors > 0)) {
      // Run final-layer mergers one at a time, to ensure limit is applied across the entire dataset.
      return false;
    }

    final int inLevel = totalMergingLevels - 2;
    final int outLevel = inLevel + 1;
    final LongSortedSet inputsReady = outputsReadyByLevel.get(inLevel);

    if (inputsReady == null) {
      return false;
    }

    final int numInputs = inputsReady.size();

    if (numInputs != getTotalMergersInLevel(inLevel)) {
      return false;
    }

    final List<ReadableFrameChannel> in = new ArrayList<>(numInputs);

    for (long i = 0; i < numInputs; i++) {
      in.add(
          levelAndRankToReadableChannelMap.get(mergerOutputFileName(inLevel, i))
                                          .getReadableChannelSupplier()
                                          .get()
                                          .getReadableFrameChannel(ultimateMergersRunSoFar)
      );
    }

    runMerger(outLevel, ultimateMergersRunSoFar++, in, ImmutableList.of());
    return true;
  }

  @GuardedBy("runWorkersLock")
  private void runMerger(
      final int level,
      final long rank,
      final List<ReadableFrameChannel> in,
      final List<PartitionedReadableFrameChannel> partitionedReadableChannelsToClose
  )
  {
    try {
      final WritableFrameChannel writableChannel;
      final MemoryAllocatorFactory frameAllocatorFactory;
      final ClusterByPartitions outPartitions;
      final String levelAndRankKey = mergerOutputFileName(level, rank);
      final boolean isFinalOutput = totalMergingLevels != UNKNOWN_LEVEL && level == totalMergingLevels - 1;

      if (isFinalOutput) {
        // Writing final partitioned output.
        final int outputPartitionCount = getOutputPartitions().size();
        final int intRank = Ints.checkedCast(rank);

        if (outputChannels == null) {
          outputChannels = Arrays.asList(new OutputChannel[outputPartitionCount]);
        }

        final OutputChannel outputChannel = outputChannelFactory.openChannel(intRank);
        writableChannel = outputChannel.getWritableChannel();
        frameAllocatorFactory = new SingleMemoryAllocatorFactory(outputChannel.getFrameMemoryAllocator());
        outputChannels.set(intRank, outputChannel.readOnly());

        if (totalMergingLevels == 1) {
          // Reading from the inputBuffer. (i.e. "direct mode"; see class-level javadoc for more details.)
          outPartitions = new ClusterByPartitions(Collections.singletonList(getOutputPartitions().get(intRank)));
        } else {
          // Reading from intermediate partitioned channels.
          outPartitions = null;
        }
      } else {
        // Writing some intermediate layer.
        PartitionedOutputChannel partitionedOutputChannel =
            intermediateOutputChannelFactory.openPartitionedChannel(levelAndRankKey, true);
        writableChannel = partitionedOutputChannel.getWritableChannel();
        frameAllocatorFactory = new SingleMemoryAllocatorFactory(partitionedOutputChannel.getFrameMemoryAllocator());

        if (level == totalMergingLevels - 2) {
          outPartitions = getOutputPartitions();
        } else {
          outPartitions = null;
        }

        // We add the readOnly() channel even though we require the writableChannel and the frame allocator because
        // the original partitionedOutputChannel would contain the reference to those, which would get cleaned up
        // appropriately and not be held up in the class level map
        levelAndRankToReadableChannelMap.put(levelAndRankKey, partitionedOutputChannel.readOnly());
      }

      final FrameChannelMerger worker =
          new FrameChannelMerger(
              in,
              frameReader,
              writableChannel,
              FrameWriters.makeRowBasedFrameWriterFactory(
                  // Row-based frames are generally preferred as inputs to mergers
                  frameAllocatorFactory,
                  frameReader.signature(),
                  // No sortColumns, because FrameChannelMerger generates frames that are sorted all on its own
                  Collections.emptyList(),
                  removeNullBytes
              ),
              sortKey,
              outPartitions,
              rowLimit
          );

      runWorker(worker, outputRows -> {
        synchronized (runWorkersLock) {
          outputsReadyByLevel.computeIfAbsent(level, ignored2 -> new LongRBTreeSet())
                             .add(rank);
          superSorterProgressTracker.addMergedBatchesForLevel(level, 1);

          if (isLimited() && totalMergingLevels != UNKNOWN_LEVEL && level == totalMergingLevels - 1) {
            rowLimit -= outputRows;

            if (rowLimit < 0) {
              throw DruidException.defensive("rowLimit[%d] below zero after outputRows[%d]", rowLimit, outputRows);
            }
          }

          for (PartitionedReadableFrameChannel partitionedReadableFrameChannel : partitionedReadableChannelsToClose) {
            try {
              partitionedReadableFrameChannel.close();
            }
            catch (IOException e) {
              throw new UncheckedIOException(
                  StringUtils.format("Could not close channel for level [%d] and rank [%d]", level, rank),
                  e
              );
            }
          }
        }
      });
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> void runWorker(final FrameProcessor<T> worker, final Consumer<T> outConsumer)
  {
    Futures.addCallback(
        exec.runFully(processorDecorator.decorate(worker), cancellationId),
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(T result)
          {
            try {
              outConsumer.accept(result);

              synchronized (runWorkersLock) {
                workerFinished();
              }
            }
            catch (Throwable e) {
              synchronized (runWorkersLock) {
                allDone.setException(e);
              }
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            synchronized (runWorkersLock) {
              allDone.setException(t);
            }
          }
        },
        // Must run in exec, instead of in the same thread, to avoid running callback immediately if the
        // worker happens to finish super-quickly.
        exec.asExecutor(cancellationId)
    );
  }

  // This also updates the progressTracker's number of total levels, and total mergers for levels. Therefore, if the
  // progressTracker is present, calling this multiple times will throw an error.
  @GuardedBy("runWorkersLock")
  private void setTotalInputFrames(final long totalInputFrames)
  {
    if (this.totalInputFrames != UNKNOWN_TOTAL) {
      throw DruidException.defensive(
          "Cannot set totalInputFrames twice (first[%s], second[%s])",
          this.totalInputFrames,
          totalInputFrames
      );
    }

    this.totalInputFrames = totalInputFrames;

    // Mark the progress tracker as trivially complete, if there is nothing to sort.
    if (totalInputFrames == 0) {
      superSorterProgressTracker.markTriviallyComplete();
    }
  }

  @GuardedBy("runWorkersLock")
  private void setTotalMergingLevelsIfPossible()
  {
    if (totalMergingLevels != UNKNOWN_LEVEL
        || totalInputFrames == UNKNOWN_TOTAL
        || !outputPartitionsFuture.isDone()) {
      return;
    }

    if (levelZeroMergersRunSoFar == 0) {
      // All frames fit in memory.
      totalMergingLevels = 1;
      superSorterProgressTracker.setTotalMergingLevels(1);
      superSorterProgressTracker.setTotalMergersForLevel(0, getOutputPartitions().size());
      return;
    }

    // Need to do a multi-level merge. Figure out how many levels.
    int level = 0;
    long inputsForLevel = totalInputFrames;

    while (inputsForLevel > maxChannelsPerMerger) {
      final long totalMergersInLevel =
          LongMath.divide(inputsForLevel, maxChannelsPerMerger, RoundingMode.CEILING);
      superSorterProgressTracker.setTotalMergersForLevel(level, totalMergersInLevel);

      level++;
      inputsForLevel = totalMergersInLevel;
    }

    if (level <= 1) {
      if (getOutputPartitions().size() > 1) {
        // Use three levels so the final merge layer (which writes the final output channels) can read from
        // a cleanly partitioned penultimate layer.
        totalMergingLevels = 3;
      } else {
        // Use two levels: no need to have a partitioned penultimate layer.
        totalMergingLevels = 2;
      }
    } else {
      totalMergingLevels = level + 1;
    }

    for (int i = level; i < totalMergingLevels; i++) {
      superSorterProgressTracker.setTotalMergersForLevel(i, 1);
    }

    superSorterProgressTracker.setTotalMergingLevels(totalMergingLevels);
  }

  private ClusterByPartitions getOutputPartitions()
  {
    if (!outputPartitionsFuture.isDone()) {
      throw new ISE("Output partitions are not ready yet");
    }

    return FutureUtils.getUnchecked(outputPartitionsFuture, true);
  }

  @GuardedBy("runWorkersLock")
  private long getTotalMergersInLevel(final int level)
  {
    if (totalInputFrames == UNKNOWN_TOTAL || totalMergingLevels == UNKNOWN_LEVEL) {
      return UNKNOWN_TOTAL;
    } else if (level >= totalMergingLevels) {
      throw new ISE("Invalid level %d", level);
    } else if (level == totalMergingLevels - 1) {
      if (outputPartitionsFuture.isDone()) {
        return totalInputFrames == 0 ? 0 : getOutputPartitions().size();
      } else {
        return UNKNOWN_TOTAL;
      }
    } else if (level > 0 && level == totalMergingLevels - 2) {
      if (outputPartitionsFuture.isDone()) {
        // Smallest number of mergers we can possibly use in the penultimate level.
        final long totalInputs = getTotalMergersInLevel(level - 1);
        final long minMergers =
            LongMath.divide(totalInputs, maxChannelsPerMerger, RoundingMode.CEILING);

        // Ensure we have a maximal degree of parallelism: possibly use more mergers than minMergers.
        long targetNumMergers = Math.max(
            minMergers,
            Math.min(
                maxActiveProcessors,
                getOutputPartitions().size()
            )
        );

        // Lower targetNumMergers if runNextMiddleManager would not actually be able to launch this many.
        return LongMath.divide(
            totalInputs,
            LongMath.divide(totalInputs, targetNumMergers, RoundingMode.CEILING),
            RoundingMode.CEILING
        );
      } else {
        return UNKNOWN_TOTAL;
      }
    } else {
      long totalMergersInLevel = totalInputFrames;

      for (int i = 0; i <= level; i++) {
        totalMergersInLevel =
            LongMath.divide(totalMergersInLevel, maxChannelsPerMerger, RoundingMode.CEILING);
      }

      return totalMergersInLevel;
    }
  }

  @GuardedBy("runWorkersLock")
  private boolean allInputRead()
  {
    return totalInputFrames != UNKNOWN_TOTAL;
  }

  /**
   * Whether this instance has finished its processing. This may be due to successful completion, or it may be due
   * to cancellation or error.
   *
   * Note: it is possible for this method to return true even when {@link #activeProcessors} is nonzero. Processors
   * take some time to exit after the instance becomes "done".
   */
  @GuardedBy("runWorkersLock")
  private boolean isAllDone()
  {
    return allDone.isDone() || allDone.isCancelled();
  }

  /**
   * Cleanup that must happen regardless of success or failure.
   */
  @GuardedBy("runWorkersLock")
  private void cleanUp()
  {
    if (!isAllDone() || activeProcessors != 0) {
      // This condition indicates a logic bug.
      throw new ISE("Improper cleanup");
    }

    if (log.isDebugEnabled()) {
      log.debug(stateString());
    }

    outputsReadyByLevel.clear();
    inputBuffer.clear();
    for (Map.Entry<String, PartitionedOutputChannel> cleanupEntry :
        levelAndRankToReadableChannelMap.entrySet()) {
      try {
        cleanupEntry.getValue().getReadableChannelSupplier().get().close();
      }
      catch (IOException e) {
        throw new UncheckedIOException("Unable to close channel for name : " + cleanupEntry.getKey(), e);
      }
    }
    levelAndRankToReadableChannelMap.clear();

    if (!inputChannelsToRead.isEmpty()) {
      for (final ReadableFrameChannel inputChannel : inputChannels) {
        CloseableUtils.closeAndSuppressExceptions(
            inputChannel::close,
            e -> log.warn(e, "Could not close input channel")
        );
      }

      inputChannels.forEach(ReadableFrameChannel::close);
    }

    inputChannelsToRead.clear();
  }

  private String mergerOutputFileName(final int level, final long rank)
  {
    return StringUtils.format("merged.%d.%d", level, rank);
  }

  /**
   * Maximum number of frames permissible in the {@link #inputBuffer} for "direct mode" (see class-level javadoc).
   */
  private int getMaxInputBufferFramesForDirectMerging()
  {
    return maxChannelsPerMerger * maxActiveProcessors;
  }

  @GuardedBy("runWorkersLock")
  private boolean isLimited()
  {
    return rowLimit != UNLIMITED;
  }

  /**
   * Returns a string encapsulating the current state of this object.
   */
  public String stateString()
  {
    synchronized (runWorkersLock) {
      return "frames-in=" + inputFramesReadSoFar + "/" + totalInputFrames
             + " frames-buffered=" + inputBuffer.size()
             + " lvls=" + totalMergingLevels
             + " parts=" +
             (outputPartitionsFuture.isDone() ? FutureUtils.getUncheckedImmediately(outputPartitionsFuture).size() : -1)
             + " p=" + activeProcessors + "/" + maxActiveProcessors
             + " ch-pending=" + inputChannelsToRead
             + " to-merge=" + outputsReadyByLevel
             + " done=" + (isAllDone() ? "y" : "n");
    }
  }

  private static ReadableFrameChannel singleReadableFrameChannel(final FrameWithPartition frame)
  {
    try {
      final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
      channel.writable().write(frame);
      channel.writable().close();
      return channel.readable();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
