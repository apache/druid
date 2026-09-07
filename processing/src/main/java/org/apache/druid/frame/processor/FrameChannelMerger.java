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

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.FrameComparisonWidget;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * Processor that merges already-sorted inputChannels and writes a fully-sorted stream to a single outputChannel.
 *
 * Frames from input channels must be {@link FrameType#isRowBased()}. Output frames will be row-based as well.
 *
 * Optionally supports combining adjacent rows with identical sort keys via a {@link FrameCombiner}, which
 * reduces intermediate data volume during merge-sort. All combine-specific state is held in {@link CombineState}.
 *
 * For unsorted output, use {@link FrameChannelMixer} instead.
 */
public class FrameChannelMerger implements FrameProcessor<Long>
{
  private static final long UNLIMITED = -1;

  private final List<ReadableFrameChannel> inputChannels;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final List<KeyColumn> sortKey;
  private final ClusterByPartitions partitions;
  private final TournamentTree tournamentTree;
  private final FrameWriterFactory frameWriterFactory;
  private final FramePlus[] currentFrames;
  private final long rowLimit;
  private long rowsOutput = 0;
  private int currentPartition = 0;

  /**
   * Non-null when combining is enabled. Holds the combiner and all pending-row state.
   */
  @Nullable
  private final CombineState combineState;

  /**
   * Channels that still have input to read.
   */
  private final IntSet remainingChannels;

  // ColumnSelectorFactory that always reads from the current row in the merged sequence.
  private final MultiColumnSelectorFactory mergedColumnSelectorFactory;

  /**
   * @param inputChannels      readable frame channels. Each channel must be sorted (i.e., if all frames in the channel
   *                           are concatenated, the concatenated result must be fully sorted).
   * @param frameReader        reader for frames
   * @param outputChannel      writable channel to receive the merge-sorted data
   * @param frameWriterFactory writer for frames
   * @param sortKey            sort key for input and output frames
   * @param combiner           optional combiner for merging rows with identical sort keys
   * @param partitions         partitions for output frames. If non-null, output frames are written with
   *                           partition numbers set according to this parameter
   * @param rowLimit           maximum number of rows to write to the output channel
   */
  public FrameChannelMerger(
      final List<ReadableFrameChannel> inputChannels,
      final FrameReader frameReader,
      final WritableFrameChannel outputChannel,
      final FrameWriterFactory frameWriterFactory,
      final List<KeyColumn> sortKey,
      @Nullable final FrameCombiner combiner,
      @Nullable final ClusterByPartitions partitions,
      final long rowLimit
  )
  {
    if (inputChannels.isEmpty()) {
      throw new IAE("Must have at least one input channel");
    }

    final ClusterByPartitions partitionsToUse =
        partitions == null ? ClusterByPartitions.oneUniversalPartition() : partitions;

    if (!partitionsToUse.allAbutting()) {
      // To simplify merging logic, when frames we only look at the earliest and latest key in "partitions". To ensure
      // correctness, we need to verify that there are no gaps.
      throw new IAE("Partitions must all abut each other");
    }

    if (!sortKey.stream().allMatch(keyColumn -> keyColumn.order().sortable())) {
      throw new IAE("Key is not sortable");
    }

    this.inputChannels = inputChannels;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.frameWriterFactory = frameWriterFactory;
    this.sortKey = sortKey;
    this.partitions = partitionsToUse;
    this.rowLimit = rowLimit;
    this.currentFrames = new FramePlus[inputChannels.size()];
    this.remainingChannels = new IntAVLTreeSet(IntSets.fromTo(0, inputChannels.size()));
    this.tournamentTree = new TournamentTree(
        inputChannels.size(),
        (k1, k2) -> {
          final FramePlus frame1 = currentFrames[k1];
          final FramePlus frame2 = currentFrames[k2];

          if (frame1 == frame2) {
            return 0;
          } else if (frame1 == null) {
            return 1;
          } else if (frame2 == null) {
            return -1;
          } else {
            return currentFrames[k1].comparisonWidget.compare(
                currentFrames[k1].rowNumber(),
                currentFrames[k2].comparisonWidget,
                currentFrames[k2].rowNumber()
            );
          }
        }
    );

    final List<Supplier<ColumnSelectorFactory>> frameColumnSelectorFactorySuppliers =
        new ArrayList<>(inputChannels.size() + (combiner != null ? 1 : 0));

    for (int i = 0; i < inputChannels.size(); i++) {
      final int frameNumber = i;
      frameColumnSelectorFactorySuppliers.add(() -> currentFrames[frameNumber].cursor.getColumnSelectorFactory());
    }

    if (combiner != null) {
      combiner.init(frameReader);
      frameColumnSelectorFactorySuppliers.add(combiner::getCombinedColumnSelectorFactory);
      this.combineState = new CombineState(combiner, inputChannels.size());
    } else {
      this.combineState = null;
    }

    this.mergedColumnSelectorFactory = new MultiColumnSelectorFactory(
        frameColumnSelectorFactorySuppliers,
        frameReader.signature()
    ).withRowMemoryAndSignatureColumns();
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return inputChannels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    final IntSet awaitSet = populateCurrentFramesAndTournamentTree();

    if (!awaitSet.isEmpty()) {
      return ReturnOrAwait.awaitAll(awaitSet);
    }

    // After populateCurrentFramesAndTournamentTree(), check if we're done.
    if (doneReadingInput() && !hasPendingCombineRow()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    }

    // Generate one output frame and stop for now.
    writeNextFrame();

    // After writeNextFrame(), check if we're done.
    if (doneReadingInput() && !hasPendingCombineRow()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  private void writeNextFrame() throws IOException
  {
    if (doneReadingInput() && !hasPendingCombineRow()) {
      throw new NoSuchElementException();
    }

    try (final FrameWriter mergedFrameWriter = frameWriterFactory.newFrameWriter(mergedColumnSelectorFactory)) {
      int mergedFramePartition = currentPartition;
      RowKey currentPartitionEnd = partitions.get(currentPartition).getEnd();

      if (combineState != null) {
        writeNextFrameWithCombiner(mergedFrameWriter, mergedFramePartition, currentPartitionEnd);
      } else {
        writeNextFrameNoCombiner(mergedFrameWriter, mergedFramePartition, currentPartitionEnd);
      }
    }
  }

  /**
   * Merge logic without combining.
   */
  private void writeNextFrameNoCombiner(
      final FrameWriter mergedFrameWriter,
      int mergedFramePartition,
      RowKey currentPartitionEnd
  ) throws IOException
  {
    while (!doneReadingInput()) {
      final int currentChannel = tournamentTree.getMin();
      mergedColumnSelectorFactory.setCurrentFactory(currentChannel);

      if (currentPartitionEnd != null) {
        final FramePlus currentFrame = currentFrames[currentChannel];
        if (currentFrame.comparisonWidget.compare(currentFrame.rowNumber(), currentPartitionEnd) >= 0) {
          // Current key is past the end of the partition. Advance currentPartition til it matches the current key.
          do {
            currentPartition++;
            currentPartitionEnd = partitions.get(currentPartition).getEnd();
          } while (currentPartitionEnd != null
                   && currentFrame.comparisonWidget.compare(currentFrame.rowNumber(), currentPartitionEnd) >= 0);

          if (mergedFrameWriter.getNumRows() == 0) {
            // Fall through: keep reading into the new partition.
            mergedFramePartition = currentPartition;
          } else {
            // Write current frame.
            break;
          }
        }
      }

      if (mergedFrameWriter.addSelection()) {
        rowsOutput++;
      } else {
        if (mergedFrameWriter.getNumRows() == 0) {
          throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
        }

        // Frame is full. Write the current frame.
        break;
      }

      if (rowLimit != UNLIMITED && rowsOutput >= rowLimit) {
        // Limit reached; we're done.
        Arrays.fill(currentFrames, null);
        remainingChannels.clear();
      } else if (!advanceChannel(currentChannel)) {
        // Channel not ready; finish up the current frame.
        break;
      }
    }

    final Frame nextFrame = Frame.wrap(mergedFrameWriter.toByteArray());
    outputChannel.write(nextFrame, mergedFramePartition);
  }

  /**
   * Merge logic with combining: buffers a "pending" row and combines adjacent rows with the same key.
   *
   * Each iteration of the outer loop completes at most one group and writes it to the frame. The logic may
   * break early if not enough input channels are ready to generate a group.
   */
  private void writeNextFrameWithCombiner(
      final FrameWriter mergedFrameWriter,
      int mergedFramePartition,
      RowKey currentPartitionEnd
  ) throws IOException
  {
    OUTER:
    while (!doneReadingInput() || combineState.hasPendingRow()) {
      // Step 1: Ensure there is a pending row.
      if (!combineState.hasPendingRow()) {
        final int currentChannel = tournamentTree.getMin();
        final FramePlus currentFrame = currentFrames[currentChannel];
        combineState.savePending(currentFrame, currentChannel);
        if (!advanceChannel(currentChannel)) {
          break; // Channel not ready; pending saved, will resume next call.
        }
      }

      // Step 2: Fold in subsequent rows with the same sort key.
      while (!doneReadingInput()) {
        final int currentChannel = tournamentTree.getMin();
        final FramePlus currentFrame = currentFrames[currentChannel];
        if (combineState.comparePendingKey(currentFrame.comparisonWidget, currentFrame.rowNumber()) != 0) {
          break; // Different key; group is complete.
        }
        if (!combineState.pendingCombined) {
          combineState.combiner.reset(combineState.pendingFrame, combineState.pendingRow);
          combineState.pendingCombined = true;
        }
        combineState.combiner.combine(currentFrame.frame, currentFrame.rowNumber());
        if (!advanceChannel(currentChannel)) {
          break OUTER; // Channel not ready; group may be incomplete.
        }
      }

      // Step 3: Check whether the pending row crosses a partition boundary.
      if (currentPartitionEnd != null && combineState.comparePendingToPartitionEnd(currentPartitionEnd) >= 0) {
        do {
          currentPartition++;
          currentPartitionEnd = partitions.get(currentPartition).getEnd();
        } while (currentPartitionEnd != null && combineState.comparePendingToPartitionEnd(currentPartitionEnd) >= 0);

        if (mergedFrameWriter.getNumRows() > 0) {
          break; // Frame has rows from the previous partition; write it first.
        }
        mergedFramePartition = currentPartition;
      }

      // Step 4: Flush the completed group to the frame writer.
      if (!flushPendingCombineRow(mergedFrameWriter)) {
        break; // Frame is full.
      }

      // Step 5: Check the row limit.
      if (rowLimit != UNLIMITED && rowsOutput >= rowLimit) {
        Arrays.fill(currentFrames, null);
        remainingChannels.clear();
        break;
      }
    }

    if (mergedFrameWriter.getNumRows() > 0) {
      final Frame nextFrame = Frame.wrap(mergedFrameWriter.toByteArray());
      outputChannel.write(nextFrame, mergedFramePartition);
    }
  }

  /**
   * Flush the pending row to the frame writer. Returns true if the row was written or did not exist (and therefore
   * did not need to be written). Returns false if the frame is full. Only used when combining, i.e. when
   * {@link #combineState} is nonnull.
   */
  private boolean flushPendingCombineRow(final FrameWriter mergedFrameWriter)
  {
    if (!combineState.hasPendingRow()) {
      return true;
    }

    final boolean didAdd;

    if (combineState.pendingCombined) {
      // Combined row: write via combiner's ColumnSelectorFactory.
      mergedColumnSelectorFactory.setCurrentFactory(combineState.combinerSlot);
      didAdd = mergedFrameWriter.addSelection();
    } else {
      // Non-combined row: try writing using original frame to avoid needing to use the combiner.
      //noinspection ObjectEquality
      if (currentFrames[combineState.pendingChannel] != null
          && currentFrames[combineState.pendingChannel].frame == combineState.pendingFrame) {
        // Frame is still live.
        final int savedRow = currentFrames[combineState.pendingChannel].cursor.getCurrentRow();
        currentFrames[combineState.pendingChannel].cursor.setCurrentRow(combineState.pendingRow);
        mergedColumnSelectorFactory.setCurrentFactory(combineState.pendingChannel);
        didAdd = mergedFrameWriter.addSelection();
        currentFrames[combineState.pendingChannel].cursor.setCurrentRow(savedRow);
      } else {
        // Frame is no longer live, perhaps the pending row was the last row of the prior frame.
        // Write using the combiner as a fallback.
        combineState.combiner.reset(combineState.pendingFrame, combineState.pendingRow);
        mergedColumnSelectorFactory.setCurrentFactory(combineState.combinerSlot);
        didAdd = mergedFrameWriter.addSelection();
      }
    }

    if (didAdd) {
      rowsOutput++;
      combineState.clearPending();
      return true;
    } else {
      if (mergedFrameWriter.getNumRows() == 0) {
        throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
      }
      return false;
    }
  }

  /**
   * Whether there is a pending row waiting to be flushed (only possible when combining).
   */
  private boolean hasPendingCombineRow()
  {
    return combineState != null && combineState.hasPendingRow();
  }

  /**
   * Advance the cursor for a channel, loading a new frame if necessary. Returns true if the channel was
   * successfully advanced (more rows available, channel finished, or next frame loaded). Returns false if
   * the channel is not ready to read and not finished, meaning the caller should finish up the current
   * output frame and wait for more data.
   */
  private boolean advanceChannel(final int currentChannel)
  {
    final FramePlus channelFramePlus = currentFrames[currentChannel];
    channelFramePlus.cursor.advance();

    if (channelFramePlus.isDone()) {
      // Done reading current frame from "channel".
      // Clear it and see if there is another one available for immediate loading.
      currentFrames[currentChannel] = null;

      final ReadableFrameChannel channel = inputChannels.get(currentChannel);

      if (channel.canRead()) {
        // Read next frame from this channel.
        final Frame frame = channel.readFrame();
        final FramePlus framePlus = makeFramePlus(frame, frameReader);
        if (framePlus.isDone()) {
          // Nothing to read in this frame, can't continue.
          return false;
        }
        currentFrames[currentChannel] = framePlus;
      } else if (channel.isFinished()) {
        // Done reading this channel.
        remainingChannels.remove(currentChannel);
      } else {
        // Nothing available, not finished; we can't continue. Caller should finish up the current frame.
        return false;
      }
    }

    return true;
  }

  /**
   * Returns whether all input is done being read.
   */
  private boolean doneReadingInput()
  {
    return remainingChannels.isEmpty();
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }

  /**
   * Populates {@link #currentFrames}, wherever necessary, from any readable input channels. Returns the set of
   * channels that are required for population but are not readable.
   */
  private IntSet populateCurrentFramesAndTournamentTree()
  {
    final IntSet await = new IntOpenHashSet();

    for (int i = 0; i < inputChannels.size(); i++) {
      if (currentFrames[i] == null && remainingChannels.contains(i)) {
        final ReadableFrameChannel channel = inputChannels.get(i);

        if (channel.canRead()) {
          final Frame frame = channel.readFrame();
          final FramePlus framePlus = makeFramePlus(frame, frameReader);
          if (framePlus.isDone()) {
            await.add(i);
          } else {
            currentFrames[i] = framePlus;
          }
        } else if (channel.isFinished()) {
          remainingChannels.remove(i);
        } else {
          await.add(i);
        }
      }
    }

    return await;
  }

  /**
   * Creates a {@link FramePlus} with start and end row set to match {@link #partitions}.
   */
  private FramePlus makeFramePlus(
      final Frame frame,
      final FrameReader frameReader
  )
  {
    final FrameCursor cursor = FrameProcessors.makeCursor(frame, frameReader);
    final FrameComparisonWidget comparisonWidget = frameReader.makeComparisonWidget(frame, sortKey);
    cursor.setCurrentRow(findRow(frame, comparisonWidget, partitions.get(0).getStart()));

    final RowKey endRowKey = partitions.get(partitions.size() - 1).getEnd();
    final int endRow;

    if (endRowKey == null) {
      endRow = frame.numRows();
    } else {
      endRow = findRow(frame, comparisonWidget, endRowKey);
    }

    return new FramePlus(frame, cursor, comparisonWidget, endRow);
  }

  /**
   * Find the first row in a frame with a key equal to, or greater than, the provided key. Returns 0 if the input
   * key is null.
   */
  static int findRow(
      final Frame frame,
      final FrameComparisonWidget comparisonWidget,
      @Nullable final RowKey key
  )
  {
    if (key == null) {
      return 0;
    }

    int minIndex = 0;
    int maxIndex = frame.numRows();

    while (minIndex < maxIndex) {
      final int currIndex = (minIndex + maxIndex) / 2;
      final int cmp = comparisonWidget.compare(currIndex, key);

      if (cmp < 0) {
        minIndex = currIndex + 1;
      } else {
        maxIndex = currIndex;
      }
    }

    return minIndex;
  }

  /**
   * Class that encapsulates the apparatus necessary for reading a {@link Frame}.
   */
  private static class FramePlus
  {
    private final Frame frame;
    private final FrameCursor cursor;
    private final FrameComparisonWidget comparisonWidget;
    private final int endRow;

    public FramePlus(
        final Frame frame,
        final FrameCursor cursor,
        final FrameComparisonWidget comparisonWidget,
        final int endRow
    )
    {
      this.frame = frame;
      this.cursor = cursor;
      this.comparisonWidget = comparisonWidget;
      this.endRow = endRow;
    }

    public int rowNumber()
    {
      return cursor.getCurrentRow();
    }

    public boolean isDone()
    {
      return cursor.getCurrentRow() >= endRow;
    }
  }

  /**
   * Holds all state related to combine-during-merge. This includes the {@link FrameCombiner} itself,
   * the index of the combiner's ColumnSelectorFactory slot in the {@link MultiColumnSelectorFactory}, and the
   * "pending row" that is buffered while we check whether the next row has the same key.
   */
  private static class CombineState
  {
    /**
     * The combiner for this run.
     */
    private final FrameCombiner combiner;

    /**
     * Index of the combiner's {@link ColumnSelectorFactory} within the {@link MultiColumnSelectorFactory}.
     * Equal to the number of input channels (i.e., one past the last channel slot).
     */
    private final int combinerSlot;

    /**
     * Whether there is a pending row buffered.
     */
    private boolean hasPendingRow;

    /**
     * Channel index that the pending row came from. Used by {@code flushPending()} to attempt the fast path:
     * if the channel's current {@link FramePlus} still references {@link #pendingFrame}, we can write the row
     * directly from the channel's cursor without involving the combiner. Only valid if {@link #hasPendingRow}.
     */
    private int pendingChannel;

    /**
     * Row number within {@link #pendingFrame} for the pending row. Only valid if {@link #hasPendingRow}.
     */
    private int pendingRow;

    /**
     * Frame reference for the pending row. Kept alive so the pending row's data can be read even after
     * the channel has advanced to a new frame. Only valid if {@link #hasPendingRow}.
     */
    @Nullable
    private Frame pendingFrame;

    /**
     * Comparison widget for {@link #pendingFrame}. Only valid if {@link #hasPendingRow}.
     */
    @Nullable
    private FrameComparisonWidget pendingComparisonWidget;

    /**
     * Whether {@link FrameCombiner#combine} has been called for the current pending row. When false,
     * the pending row is a singleton that has not yet been passed to the combiner. In this case,
     * {@code flushPending()} can use the fast path of writing directly from the original frame.
     * When true, the combiner holds the accumulated state and must be used for writing.
     */
    private boolean pendingCombined;

    CombineState(final FrameCombiner combiner, final int combinerSlot)
    {
      this.combiner = combiner;
      this.combinerSlot = combinerSlot;
    }

    /**
     * Whether a row is currently pending.
     */
    boolean hasPendingRow()
    {
      return hasPendingRow;
    }

    /**
     * Update the pending row.
     */
    void savePending(
        final FramePlus framePlus,
        final int channel
    )
    {
      this.hasPendingRow = true;
      this.pendingChannel = channel;
      this.pendingRow = framePlus.rowNumber();
      this.pendingFrame = framePlus.frame;
      this.pendingComparisonWidget = framePlus.comparisonWidget;
      this.pendingCombined = false;
    }

    /**
     * Clear the pending row.
     */
    void clearPending()
    {
      this.hasPendingRow = false;
      this.pendingFrame = null;
      this.pendingComparisonWidget = null;
    }

    /**
     * Compare the pending row's key against a row from another frame. Uses frame-to-frame comparison
     * to avoid materializing the key as a byte array.
     */
    int comparePendingKey(final FrameComparisonWidget otherWidget, final int otherRow)
    {
      return pendingComparisonWidget.compare(pendingRow, otherWidget, otherRow);
    }

    /**
     * Compare the pending row to a partition boundary key.
     */
    int comparePendingToPartitionEnd(final RowKey partitionEnd)
    {
      return pendingComparisonWidget.compare(pendingRow, partitionEnd);
    }
  }
}
