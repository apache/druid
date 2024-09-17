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
import org.apache.druid.frame.channel.FrameWithPartition;
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
 * Frames from input channels must be {@link org.apache.druid.frame.FrameType#ROW_BASED}. Output frames will
 * be row-based as well.
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
   * Channels that still have input to read.
   */
  private final IntSet remainingChannels;

  // ColumnSelectorFactory that always reads from the current row in the merged sequence.
  final MultiColumnSelectorFactory mergedColumnSelectorFactory;

  /**
   * @param inputChannels      readable frame channels. Each channel must be sorted (i.e., if all frames in the channel
   *                           are concatenated, the concatenated result must be fully sorted).
   * @param frameReader        reader for frames
   * @param outputChannel      writable channel to receive the merge-sorted data
   * @param frameWriterFactory writer for frames
   * @param sortKey            sort key for input and output frames
   * @param partitions         partitions for output frames. If non-null, output frames are written with
   *                           {@link FrameWithPartition#partition()} set according to this parameter
   * @param rowLimit           maximum number of rows to write to the output channel
   */
  public FrameChannelMerger(
      final List<ReadableFrameChannel> inputChannels,
      final FrameReader frameReader,
      final WritableFrameChannel outputChannel,
      final FrameWriterFactory frameWriterFactory,
      final List<KeyColumn> sortKey,
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
        new ArrayList<>(inputChannels.size());

    for (int i = 0; i < inputChannels.size(); i++) {
      final int frameNumber = i;
      frameColumnSelectorFactorySuppliers.add(() -> currentFrames[frameNumber].cursor.getColumnSelectorFactory());
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

    // Check finished() after populateCurrentFramesAndTournamentTree().
    if (finished()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    }

    // Generate one output frame and stop for now.
    outputChannel.write(nextFrame());

    // Check finished() after nextFrame().
    if (finished()) {
      return ReturnOrAwait.returnObject(rowsOutput);
    } else {
      return ReturnOrAwait.runAgain();
    }
  }

  private FrameWithPartition nextFrame()
  {
    if (finished()) {
      throw new NoSuchElementException();
    }

    try (final FrameWriter mergedFrameWriter = frameWriterFactory.newFrameWriter(mergedColumnSelectorFactory)) {
      int mergedFramePartition = currentPartition;
      RowKey currentPartitionEnd = partitions.get(currentPartition).getEnd();

      while (!finished()) {
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
              // Return current frame.
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

          // Frame is full. Return the current frame.
          break;
        }

        if (rowLimit != UNLIMITED && rowsOutput >= rowLimit) {
          // Limit reached; we're done.
          Arrays.fill(currentFrames, null);
          remainingChannels.clear();
        } else {
          // Continue reading the currentChannel.
          final FramePlus channelFramePlus = currentFrames[currentChannel];
          channelFramePlus.cursor.advance();

          if (channelFramePlus.isDone()) {
            // Done reading current frame from "channel".
            // Clear it and see if there is another one available for immediate loading.
            currentFrames[currentChannel] = null;

            final ReadableFrameChannel channel = inputChannels.get(currentChannel);

            if (channel.canRead()) {
              // Read next frame from this channel.
              final Frame frame = channel.read();
              final FramePlus framePlus = makeFramePlus(frame, frameReader);
              if (framePlus.isDone()) {
                // Nothing to read in this frame. Not finished; we can't continue.
                // Finish up the current frame and return it.
                break;
              } else {
                currentFrames[currentChannel] = framePlus;
              }
            } else if (channel.isFinished()) {
              // Done reading this channel. Fall through and continue with other channels.
              remainingChannels.remove(currentChannel);
            } else {
              // Nothing available, not finished; we can't continue. Finish up the current frame and return it.
              break;
            }
          }
        }
      }

      final Frame nextFrame = Frame.wrap(mergedFrameWriter.toByteArray());
      return new FrameWithPartition(nextFrame, mergedFramePartition);
    }
  }

  /**
   * Returns whether all input is done being read.
   */
  private boolean finished()
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
          final Frame frame = channel.read();
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

    return new FramePlus(cursor, comparisonWidget, endRow);
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
    private final FrameCursor cursor;
    private final FrameComparisonWidget comparisonWidget;
    private final int endRow;

    public FramePlus(
        final FrameCursor cursor,
        final FrameComparisonWidget comparisonWidget,
        final int endRow
    )
    {
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
}
