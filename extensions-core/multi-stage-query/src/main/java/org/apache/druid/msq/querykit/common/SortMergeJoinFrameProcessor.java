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

package org.apache.druid.msq.querykit.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.key.FrameComparisonWidget;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyRowsWithSameKeyFault;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.JoinType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Processor for a sort-merge join of two inputs.
 *
 * Prerequisites:
 *
 * 1) Two inputs, both of which are stages; i.e. {@link ReadableInput#hasChannel()}.
 *
 * 2) Conditions are all simple equalities. Validated by {@link SortMergeJoinStageProcessor#validateCondition}
 * and then transformed to lists of key columns by {@link SortMergeJoinStageProcessor#toKeyColumns}.
 *
 * 3) Both inputs are comprised of {@link org.apache.druid.frame.FrameType#ROW_BASED} frames, are sorted by the same
 * key, and that key can be used to check the provided condition. Validated by
 * {@link SortMergeJoinStageProcessor#validateInputFrameSignatures}.
 *
 * Algorithm:
 *
 * 1) Read current key from each side of the join.
 *
 * 2) If there is no match, emit or skip the row for the earlier key, as appropriate, based on the join type.
 *
 * 3) If there is a match, identify a complete set on one side or the other. (It doesn't matter which side has the
 * complete set, but we need it on one of them.) We mark the first row for the key using {@link Tracker#markCurrent()}
 * and find complete sets using {@link Tracker#hasCompleteSetForMark()}. Once we find one, we store it in
 * {@link #trackerWithCompleteSetForCurrentKey}. If both sides have a complete set, we break ties by choosing the
 * left side.
 *
 * 4) Once a complete set for the current key is identified: for each row on the *other* side, loop through the entire
 * set of rows on {@link #trackerWithCompleteSetForCurrentKey}, and emit that many joined rows.
 *
 * 5) Once we process the final row on the *other* side, reset both marks with {@link Tracker#markCurrent()} and
 * continue the algorithm.
 */
public class SortMergeJoinFrameProcessor implements FrameProcessor<Object>
{
  private static final int LEFT = 0;
  private static final int RIGHT = 1;

  /**
   * Input channels for each side of the join. Two-element array: {@link #LEFT} and {@link #RIGHT}.
   */
  private final List<ReadableFrameChannel> inputChannels;

  /**
   * Trackers for each side of the join. Two-element array: {@link #LEFT} and {@link #RIGHT}.
   */
  private final List<Tracker> trackers;

  private final WritableFrameChannel outputChannel;
  private final FrameWriterFactory frameWriterFactory;
  private final String rightPrefix;
  private final JoinType joinType;
  private final JoinColumnSelectorFactory joinColumnSelectorFactory = new JoinColumnSelectorFactory();
  private final long maxBufferedBytes;
  private FrameWriter frameWriter = null;

  // Used by runIncrementally to defer certain logic to the next run.
  private Runnable nextIterationRunnable = null;

  // Used by runIncrementally to remember which tracker has the complete set for the current key.
  private int trackerWithCompleteSetForCurrentKey = -1;

  SortMergeJoinFrameProcessor(
      ReadableInput left,
      ReadableInput right,
      WritableFrameChannel outputChannel,
      FrameWriterFactory frameWriterFactory,
      String rightPrefix,
      List<List<KeyColumn>> keyColumns,
      int[] requiredNonNullKeyParts,
      JoinType joinType,
      long maxBufferedBytes
  )
  {
    this.inputChannels = ImmutableList.of(left.getChannel(), right.getChannel());
    this.outputChannel = outputChannel;
    this.frameWriterFactory = frameWriterFactory;
    this.rightPrefix = rightPrefix;
    this.joinType = joinType;
    this.trackers = ImmutableList.of(
        new Tracker(left, keyColumns.get(LEFT), requiredNonNullKeyParts, maxBufferedBytes),
        new Tracker(right, keyColumns.get(RIGHT), requiredNonNullKeyParts, maxBufferedBytes)
    );
    this.maxBufferedBytes = maxBufferedBytes;
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
  public ReturnOrAwait<Object> runIncrementally(IntSet readableInputs) throws IOException
  {
    // Fetch enough frames such that each tracker has one readable row (or is done).
    for (int i = 0; i < inputChannels.size(); i++) {
      final Tracker tracker = trackers.get(i);
      if (tracker.needsMoreDataForCurrentCursor() && !pushNextFrame(i)) {
        return nextAwait();
      }
    }

    // Initialize new output frame, if needed.
    startNewFrameIfNeeded();

    while (!allTrackersAreAtEnd()
           && !trackers.get(LEFT).needsMoreDataForCurrentCursor()
           && !trackers.get(RIGHT).needsMoreDataForCurrentCursor()) {
      // Algorithm can proceed: not all trackers are at the end of their streams, and no tracker needs more data to
      // read the current cursor or move it forward.
      if (nextIterationRunnable != null) {
        final Runnable tmp = nextIterationRunnable;
        nextIterationRunnable = null;
        tmp.run();
      }

      final int markCmp = compareMarks();

      // Two rows match if the keys compare equal _and_ neither key has a null component. (x JOIN y ON x.a = y.a does
      // not match rows where "x.a" is null.)
      final boolean marksMatch = markCmp == 0 && trackers.get(LEFT).markHasRequiredNonNullKeyParts();

      // If marked keys are equal on both sides ("marksMatch"), at least one side needs to have a complete set of rows
      // for the marked key. Check if this is true, otherwise call nextAwait to read more data.
      if (marksMatch && trackerWithCompleteSetForCurrentKey < 0) {
        updateTrackerWithCompleteSetForCurrentKey();

        if (trackerWithCompleteSetForCurrentKey < 0) {
          // Algorithm cannot proceed; fetch more frames on the next run.
          return nextAwait();
        }
      }

      // Emit row if there was a match.
      if (!emitRowIfNeeded(markCmp, marksMatch)) {
        return ReturnOrAwait.runAgain();
      }

      // Advance one or both trackers.
      advanceTrackersAfterEmittingRow(markCmp, marksMatch);
    }

    if (allTrackersAreAtEnd()) {
      flushCurrentFrame();
      return ReturnOrAwait.returnObject(Unit.instance());
    } else {
      // Keep reading.
      return nextAwait();
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), frameWriter, () -> trackers.forEach(Tracker::clear));
  }

  /**
   * Set {@link #trackerWithCompleteSetForCurrentKey} to the lowest-numbered {@link Tracker} that has a complete
   * set of rows available for its mark.
   */
  private void updateTrackerWithCompleteSetForCurrentKey()
  {
    for (int i = 0; i < inputChannels.size(); i++) {
      final Tracker tracker = trackers.get(i);

      // Fetch up to one frame from each tracker, to check if that tracker has a complete set.
      // Can't fetch more than one frame, because channels are only guaranteed to have one frame per run.
      if (tracker.hasCompleteSetForMark() || (pushNextFrame(i) && tracker.hasCompleteSetForMark())) {
        trackerWithCompleteSetForCurrentKey = i;
        return;
      }
    }

    trackerWithCompleteSetForCurrentKey = -1;
  }

  /**
   * Emits a joined row based on the current state of all trackers.
   *
   * @param markCmp    result of {@link #compareMarks()}
   * @param marksMatch whether the marks actually matched, taking nulls into account
   *
   * @return true if cursors should be advanced, false if we should run again without moving cursors
   */
  private boolean emitRowIfNeeded(final int markCmp, final boolean marksMatch) throws IOException
  {
    if (marksMatch || (markCmp <= 0 && joinType.isLefty()) || (markCmp >= 0 && joinType.isRighty())) {
      // Emit row, if there's room in the current frameWriter.
      joinColumnSelectorFactory.cmp = markCmp;
      joinColumnSelectorFactory.match = marksMatch;

      if (!frameWriter.addSelection()) {
        if (frameWriter.getNumRows() > 0) {
          // Out of space in the current frame. Run again without moving cursors.
          flushCurrentFrame();
          return false;
        } else {
          throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
        }
      }
    }

    return true;
  }

  /**
   * Advance one or both trackers after emitting a row.
   *
   * @param markCmp    result of {@link #compareMarks()}
   * @param marksMatch whether the marks actually matched, taking nulls into account
   */
  private void advanceTrackersAfterEmittingRow(final int markCmp, final boolean marksMatch)
  {
    if (marksMatch) {
      // Matching keys. First advance the tracker with the complete set.
      final Tracker completeSetTracker = trackers.get(trackerWithCompleteSetForCurrentKey);
      final Tracker otherTracker = trackers.get(trackerWithCompleteSetForCurrentKey == LEFT ? RIGHT : LEFT);

      completeSetTracker.advance();
      if (!completeSetTracker.isCurrentSameKeyAsMark()) {
        // Reached end of complete set. Advance the other tracker.
        otherTracker.advance();

        // On next iteration (when we're sure to have data) either rewind the complete-set tracker, or update marks
        // of both, as appropriate.
        onNextIteration(() -> {
          if (otherTracker.isCurrentSameKeyAsMark()) {
            completeSetTracker.rewindToMark();
          } else {
            // Reached end of the other side too. Advance marks on both trackers.
            completeSetTracker.markCurrent();
            trackerWithCompleteSetForCurrentKey = -1;
          }

          // Always update mark of the other tracker, to enable cleanup of old frames. It doesn't ever need to
          // be rewound.
          otherTracker.markCurrent();
        });
      }
    } else {
      // Keys don't match. Advance based on what kind of join this is.
      final int trackerToAdvance;
      final boolean skipMarkedKey;

      if (markCmp < 0) {
        trackerToAdvance = LEFT;
      } else if (markCmp > 0) {
        trackerToAdvance = RIGHT;
      } else {
        // Key is null on both sides. Note that there is a preference for running through the left side first
        // on a FULL join. It doesn't really matter which side we run through first, but we do need to be consistent
        // for the benefit of the logic in "shouldEmitColumnValue".
        trackerToAdvance = joinType.isLefty() ? LEFT : RIGHT;
      }

      // Skip marked key entirely if we're on the "off" side of the join. (i.e., right side of a LEFT join.)
      // Note that for FULL joins, entire keys are never skipped, because they are both lefty and righty.
      if (trackerToAdvance == LEFT) {
        skipMarkedKey = !joinType.isLefty();
      } else {
        skipMarkedKey = !joinType.isRighty();
      }

      final Tracker tracker = trackers.get(trackerToAdvance);

      // Advance past marked key, or as far as we can.
      boolean didKeyChange = false;

      do {
        // Always advance a single row. If we're in "skipMarkedKey" mode, then we'll loop through later and
        // potentially skip multiple rows with the same marked key.
        tracker.advance();

        if (tracker.isAtEndOfPushedData()) {
          break;
        }

        didKeyChange = !tracker.isCurrentSameKeyAsMark();

        // Always update mark, even if key hasn't changed, to enable cleanup of old frames.
        tracker.markCurrent();
      } while (skipMarkedKey && !didKeyChange);

      if (didKeyChange) {
        trackerWithCompleteSetForCurrentKey = -1;
      } else if (tracker.isAtEndOfPushedData()) {
        // Not clear if we reached a new key or not.
        // So, on next iteration (when we're sure to have data), check if we've moved on to a new key.
        onNextIteration(() -> {
          if (!tracker.isCurrentSameKeyAsMark()) {
            trackerWithCompleteSetForCurrentKey = -1;
          }

          // Always update mark, even if key hasn't changed, to enable cleanup of old frames.
          tracker.markCurrent();
        });
      }
    }
  }

  /**
   * Returns a {@link ReturnOrAwait#awaitAll} for channels where {@link Tracker#needsMoreDataForCurrentCursor()}
   * and {@link Tracker#canBufferMoreFrames()}.
   *
   * If all channels have hit their limit, throws {@link MSQException} with {@link TooManyRowsWithSameKeyFault}.
   */
  private ReturnOrAwait<Object> nextAwait()
  {
    final IntSet awaitSet = new IntOpenHashSet();
    int trackerAtLimit = -1;

    // Add all trackers that "needsMoreData" to awaitSet.
    for (int i = 0; i < inputChannels.size(); i++) {
      final Tracker tracker = trackers.get(i);
      if (tracker.needsMoreDataForCurrentCursor()) {
        if (tracker.canBufferMoreFrames()) {
          awaitSet.add(i);
        } else if (trackerAtLimit < 0) {
          trackerAtLimit = i;
        }
      }
    }

    if (awaitSet.isEmpty()) {
      // No tracker reported that it "needsMoreData" to read the current cursor. However, we may still need to read
      // more data to have a complete set for the current mark.
      for (int i = 0; i < inputChannels.size(); i++) {
        final Tracker tracker = trackers.get(i);
        if (!tracker.hasCompleteSetForMark()) {
          if (tracker.canBufferMoreFrames()) {
            awaitSet.add(i);
          } else if (trackerAtLimit < 0) {
            trackerAtLimit = i;
          }
        }
      }
    }

    if (awaitSet.isEmpty() && trackerAtLimit >= 0) {
      // All trackers that need more data are at their max buffered bytes limit. Generate a nice exception.
      final Tracker tracker = trackers.get(trackerAtLimit);
      throw new MSQException(
          new TooManyRowsWithSameKeyFault(
              tracker.readMarkKey(),
              tracker.totalBytesBuffered(),
              maxBufferedBytes
          )
      );
    }

    return ReturnOrAwait.awaitAll(awaitSet);
  }

  /**
   * Whether all trackers return true from {@link Tracker#isAtEnd()}.
   */
  private boolean allTrackersAreAtEnd()
  {
    for (Tracker tracker : trackers) {
      if (!tracker.isAtEnd()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Compares the marked rows of the two {@link #trackers}. This method returns 0 if both sides are null, even
   * though this is not considered a match by join semantics. Therefore, it is important to also check
   * {@link Tracker#markHasRequiredNonNullKeyParts()}.
   *
   * @return negative if {@link #LEFT} key is earlier, positive if {@link #RIGHT} key is earlier, zero if the keys
   * are the same. Returns zero even if a key component is null, even though this is not considered a match by
   * join semantics.
   *
   * @throws IllegalStateException if either tracker does not have a marked row and is not completely done
   */
  private int compareMarks()
  {
    final Tracker leftTracker = trackers.get(LEFT);
    final Tracker rightTracker = trackers.get(RIGHT);

    Preconditions.checkState(leftTracker.hasMark() || leftTracker.isAtEnd(), "left.hasMark || left.isAtEnd");
    Preconditions.checkState(rightTracker.hasMark() || rightTracker.isAtEnd(), "right.hasMark || right.isAtEnd");

    if (!leftTracker.hasMark()) {
      return rightTracker.markFrame < 0 ? 0 : 1;
    } else if (!rightTracker.hasMark()) {
      return -1;
    } else {
      final FrameHolder leftHolder = leftTracker.holders.get(leftTracker.markFrame);
      final FrameHolder rightHolder = rightTracker.holders.get(rightTracker.markFrame);
      return leftHolder.comparisonWidget.compare(
          leftTracker.markRow,
          rightHolder.comparisonWidget,
          rightTracker.markRow
      );
    }
  }

  /**
   * Pushes a frame from the indicated channel into the appropriate tracker. Returns true if a frame was pushed
   * or if the channel is finished.
   */
  private boolean pushNextFrame(final int channelNumber)
  {
    final ReadableFrameChannel channel = inputChannels.get(channelNumber);
    final Tracker tracker = trackers.get(channelNumber);

    if (!channel.isFinished() && !channel.canRead()) {
      return false;
    } else if (channel.isFinished()) {
      tracker.push(null);
      return true;
    } else if (!tracker.canBufferMoreFrames()) {
      return false;
    } else {
      final Frame frame = channel.read();

      if (frame.numRows() == 0) {
        // Skip, read next.
        return false;
      } else {
        tracker.push(frame);
        return true;
      }
    }
  }

  private void onNextIteration(final Runnable runnable)
  {
    if (nextIterationRunnable != null) {
      throw new ISE("postAdvanceRunnable already set");
    } else {
      nextIterationRunnable = runnable;
    }
  }

  private void startNewFrameIfNeeded()
  {
    if (frameWriter == null) {
      frameWriter = frameWriterFactory.newFrameWriter(joinColumnSelectorFactory);
    }
  }

  private void flushCurrentFrame() throws IOException
  {
    if (frameWriter != null) {
      if (frameWriter.getNumRows() > 0) {
        final Frame frame = Frame.wrap(frameWriter.toByteArray());
        frameWriter.close();
        frameWriter = null;
        outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
      }
    }
  }

  /**
   * Tracks the current set of rows that have the same key from a sequence of frames.
   *
   * markFrame and markRow are set when we encounter a new key, which enables rewinding and re-reading data with the
   * same key.
   */
  private static class Tracker
  {
    /**
     * Frame holders for the current frame, as well as immediately prior frames that share the same marked key.
     * Prior frames are cleared on each call to {@link #markCurrent()}.
     */
    private final List<FrameHolder> holders = new ArrayList<>();
    private final ReadableInput input;
    private final List<KeyColumn> keyColumns;
    private final int[] requiredNonNullKeyParts;
    private final long maxBytesBuffered;

    // markFrame and markRow are the first frame and row with the current key.
    private int markFrame = -1;
    private int markRow = -1;

    // currentFrame is the frame containing the current cursor row.
    private int currentFrame = -1;

    // done indicates that no more data is available in the channel.
    private boolean done;

    public Tracker(
        final ReadableInput input,
        final List<KeyColumn> keyColumns,
        final int[] requiredNonNullKeyParts,
        final long maxBytesBuffered
    )
    {
      this.input = input;
      this.keyColumns = keyColumns;
      this.requiredNonNullKeyParts = requiredNonNullKeyParts;
      this.maxBytesBuffered = maxBytesBuffered;
    }

    /**
     * Adds a holder for a frame. If this is the first frame, sets the current cursor position and mark to the first
     * row of the frame. Otherwise, the cursor position and mark are not changed.
     *
     * Pushing a null frame indicates no more frames are coming.
     *
     * @param frame frame, or null indicating no more frames are coming
     */
    public void push(final Frame frame)
    {
      if (frame == null) {
        done = true;
        return;
      }

      if (done) {
        throw new ISE("Cannot push frames when already done");
      }

      final boolean atEndOfPushedData = isAtEndOfPushedData();
      final FrameReader frameReader = input.getChannelFrameReader();
      final FrameCursor cursor = FrameProcessors.makeCursor(frame, frameReader);
      final FrameComparisonWidget comparisonWidget =
          frameReader.makeComparisonWidget(frame, keyColumns);

      final RowSignature.Builder keySignatureBuilder = RowSignature.builder();
      for (final KeyColumn keyColumn : keyColumns) {
        keySignatureBuilder.add(
            keyColumn.columnName(),
            frameReader.signature().getColumnType(keyColumn.columnName()).orElse(null)
        );
      }

      holders.add(
          new FrameHolder(
              frame,
              RowKeyReader.create(keySignatureBuilder.build()),
              cursor,
              comparisonWidget
          )
      );

      if (atEndOfPushedData) {
        // Move currentFrame so it points at the next row, which we now have, instead of an "isDone" cursor.
        currentFrame = currentFrame < 0 ? 0 : currentFrame + 1;
      }

      if (markFrame < 0) {
        // Cleared mark means we want the current row to be marked.
        markFrame = currentFrame;
        markRow = 0;
      }
    }

    /**
     * Number of bytes currently buffered in {@link #holders}.
     */
    public long totalBytesBuffered()
    {
      long bytes = 0;
      for (final FrameHolder holder : holders) {
        bytes += holder.frame.numBytes();
      }
      return bytes;
    }

    /**
     * Whether this tracker can accept more frames without exceeding {@link #maxBufferedBytes}. Always returns true
     * if the number of buffered frames is zero or one, because the join algorithm may require two frames being
     * buffered. (For example, if we need to verify that the last row in a frame contains a complete set of a key.)
     */
    public boolean canBufferMoreFrames()
    {
      return holders.size() <= 1 || totalBytesBuffered() < maxBytesBuffered;
    }

    /**
     * Cursor containing the current row.
     */
    @Nullable
    public FrameCursor currentCursor()
    {
      if (currentFrame < 0) {
        return null;
      } else {
        return holders.get(currentFrame).cursor;
      }
    }

    /**
     * Advances the current row (the current row of {@link #currentFrame}). After calling this method,
     * {@link #isAtEndOfPushedData()} may start returning true.
     */
    public void advance()
    {
      assert !isAtEndOfPushedData();

      final FrameHolder currentHolder = holders.get(currentFrame);

      currentHolder.cursor.advance();

      if (currentHolder.cursor.isDone() && currentFrame + 1 < holders.size()) {
        currentFrame++;
        holders.get(currentFrame).cursor.reset();
      }
    }

    /**
     * Whether this tracker has a marked row.
     */
    public boolean hasMark()
    {
      return markFrame >= 0;
    }

    /**
     * Whether this tracker has a marked row that is completely nonnull.
     */
    public boolean markHasRequiredNonNullKeyParts()
    {
      return hasMark() && holders.get(markFrame).comparisonWidget.hasNonNullKeyParts(markRow, requiredNonNullKeyParts);
    }

    /**
     * Reads the current marked key.
     */
    @Nullable
    public List<Object> readMarkKey()
    {
      if (!hasMark()) {
        return null;
      }

      final FrameHolder markHolder = holders.get(markFrame);
      final RowKey markKey = markHolder.comparisonWidget.readKey(markRow);
      return markHolder.keyReader.read(markKey);
    }

    /**
     * Rewind to the mark row: the first one with the current key.
     *
     * @throws IllegalStateException if there is no marked row
     */
    public void rewindToMark()
    {
      if (markFrame < 0) {
        throw new ISE("No mark");
      }

      currentFrame = markFrame;
      holders.get(currentFrame).cursor.setCurrentRow(markRow);
    }

    /**
     * Set the mark row to the current row. Used when data from the old mark to the current row is no longer needed.
     */
    public void markCurrent()
    {
      if (isAtEndOfPushedData()) {
        clear();
      } else {
        // Remove unnecessary holders, now that the mark has moved on.
        while (currentFrame > 0) {
          if (currentFrame == holders.size() - 1) {
            final FrameHolder lastHolder = holders.get(currentFrame);
            holders.clear();
            holders.add(lastHolder);
            currentFrame = 0;
          } else {
            holders.remove(0);
            currentFrame--;
          }
        }

        markFrame = 0;
        markRow = holders.get(currentFrame).cursor.getCurrentRow();
      }
    }

    /**
     * Whether the current cursor is past the end of the last frame for which we have data.
     */
    public boolean isAtEndOfPushedData()
    {
      return currentFrame < 0 || (currentFrame == holders.size() - 1 && holders.get(currentFrame).cursor.isDone());
    }

    /**
     * Whether the current cursor is past the end of all data that will ever be pushed.
     */
    public boolean isAtEnd()
    {
      return done && isAtEndOfPushedData();
    }

    /**
     * Whether this tracker needs more data in order to read the current cursor location or move it forward.
     */
    public boolean needsMoreDataForCurrentCursor()
    {
      return !done && isAtEndOfPushedData();
    }

    /**
     * Whether this tracker contains all rows for the marked key.
     *
     * @throws IllegalStateException if there is no marked key
     */
    public boolean hasCompleteSetForMark()
    {
      if (markFrame < 0) {
        throw new ISE("No mark");
      }

      if (done) {
        return true;
      }

      final FrameHolder lastHolder = holders.get(holders.size() - 1);
      return !isSameKeyAsMark(lastHolder, lastHolder.frame.numRows() - 1);
    }

    /**
     * Whether the current position (the current row of the {@link #currentFrame}) compares equally to the mark row.
     * If {@link #isAtEnd()}, returns true iff there is no mark row.
     */
    public boolean isCurrentSameKeyAsMark()
    {
      if (isAtEnd()) {
        return markFrame < 0;
      } else {
        assert !isAtEndOfPushedData();
        final FrameHolder headHolder = holders.get(currentFrame);
        return isSameKeyAsMark(headHolder, headHolder.cursor.getCurrentRow());
      }
    }

    /**
     * Clears the current mark and all buffered frames. Does not change {@link #done}.
     */
    public void clear()
    {
      holders.clear();
      markFrame = -1;
      markRow = -1;
      currentFrame = -1;
    }

    /**
     * Whether the provided frame and row compares equally to the mark row. The provided row must be at, or after,
     * the mark row.
     */
    private boolean isSameKeyAsMark(final FrameHolder holder, final int row)
    {
      if (markFrame < 0) {
        throw new ISE("No marked frame");
      }
      if (row < 0 || row >= holder.frame.numRows()) {
        throw new ISE("Row [%d] out of bounds", row);
      }

      final FrameHolder markHolder = holders.get(markFrame);
      final int cmp = markHolder.comparisonWidget.compare(markRow, holder.comparisonWidget, row);

      if (cmp > 0) {
        // The provided row is at, or after, the marked row.
        // Therefore, cmp > 0 may indicate that input was provided out of order.
        throw new ISE("Row compares higher than mark; out-of-order input?");
      }

      return cmp == 0;
    }
  }

  /**
   * Selector for joined rows. This is used as an input to {@link #frameWriter}.
   */
  private class JoinColumnSelectorFactory implements ColumnSelectorFactory
  {
    /**
     * Current key comparison between left- and right-hand side.
     */
    private int cmp;

    /**
     * Whether there is a match between the left- and right-hand side. Not equivalent to {@code cmp == 0} in
     * the case where the key on both sides is null.
     */
    private boolean match;

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      if (dimensionSpec.getExtractionFn() != null || dimensionSpec.mustDecorate()) {
        // Not supported; but that's okay, because these features aren't needed when reading from this
        // ColumnSelectorFactory. It is handed to a FrameWriter, which always uses DefaultDimensionSpec.
        throw new UnsupportedOperationException();
      }

      final int channel = getChannelNumber(dimensionSpec.getDimension());
      final ColumnCapabilities columnCapabilities = getColumnCapabilities(dimensionSpec.getDimension());

      if (columnCapabilities == null) {
        // Not an output column.
        return DimensionSelector.constant(null);
      } else {
        return new JoinDimensionSelector(channel, getInputColumnName(dimensionSpec.getDimension()));
      }
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      final int channel = getChannelNumber(columnName);
      final ColumnCapabilities columnCapabilities = getColumnCapabilities(columnName);

      if (columnCapabilities == null) {
        // Not an output column.
        return NilColumnValueSelector.instance();
      } else {
        return new JoinColumnValueSelector(channel, getInputColumnName(columnName));
      }
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return frameWriterFactory.signature().getColumnCapabilities(column);
    }

    /**
     * Channel number for a possibly-prefixed column name.
     */
    private int getChannelNumber(final String column)
    {
      if (JoinPrefixUtils.isPrefixedBy(column, rightPrefix)) {
        return RIGHT;
      } else {
        return LEFT;
      }
    }

    /**
     * Unprefixed column name for a possibly-prefixed column name.
     */
    private String getInputColumnName(final String column)
    {
      if (JoinPrefixUtils.isPrefixedBy(column, rightPrefix)) {
        return JoinPrefixUtils.unprefix(column, rightPrefix);
      } else {
        return column;
      }
    }

    /**
     * Whether columns for the given channel are to be emitted with the current row.
     */
    private boolean shouldEmitColumnValue(final int channel)
    {
      // Asymmetry between left and right is necessary to properly handle FULL OUTER case where there are null keys.
      // In this case, we run through the left-hand side first, then the right-hand side.
      return !trackers.get(channel).isAtEndOfPushedData()
             && (match
                 || (channel == LEFT && joinType.isLefty() && cmp <= 0)
                 || (channel == RIGHT && joinType.isRighty()
                     && ((joinType.isLefty() && cmp > 0) || (!joinType.isLefty() && cmp >= 0))));
    }

    private class JoinDimensionSelector implements DimensionSelector
    {
      private final int channel;
      private final String columnName;

      public JoinDimensionSelector(int channel, String columnName)
      {
        this.channel = channel;
        this.columnName = columnName;
      }

      private Cursor currentCursor;
      private DimensionSelector currentSelector;

      @Nullable
      @Override
      public Object getObject()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.getObject();
        } else {
          return null;
        }
      }

      @Override
      public IndexedInts getRow()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.getRow();
        } else {
          return ZeroIndexedInts.instance();
        }
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.lookupName(id);
        } else {
          return null;
        }
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.lookupNameUtf8(id);
        } else {
          return null;
        }
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.supportsLookupNameUtf8();
        } else {
          return true;
        }
      }

      @Override
      public int getValueCardinality()
      {
        return CARDINALITY_UNKNOWN;
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return false;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return null;
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
      }

      @Override
      public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Not needed: TopN engine won't run on this.
        throw new UnsupportedOperationException();
      }

      private void refreshCursor()
      {
        final FrameCursor headCursor = trackers.get(channel).currentCursor();

        //noinspection ObjectEquality
        if (currentCursor != headCursor) {
          if (headCursor == null) {
            currentCursor = null;
            currentSelector = null;
          } else {
            currentCursor = headCursor;
            currentSelector = headCursor.getColumnSelectorFactory()
                                        .makeDimensionSelector(DefaultDimensionSpec.of(columnName));
          }
        }
      }
    }

    private class JoinColumnValueSelector implements ColumnValueSelector<Object>
    {
      private final int channel;
      private final String columnName;

      private Cursor currentCursor;
      private ColumnValueSelector<?> currentSelector;

      public JoinColumnValueSelector(int channel, String columnName)
      {
        this.channel = channel;
        this.columnName = columnName;
      }

      @Override
      public long getLong()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.getLong();
        } else {
          return 0;
        }
      }

      @Override
      public double getDouble()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.getDouble();
        } else {
          return 0;
        }
      }

      @Override
      public float getFloat()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.getFloat();
        } else {
          return 0;
        }
      }

      @Nullable
      @Override
      public Object getObject()
      {
        refreshCursor();
        if (shouldEmitColumnValue(channel)) {
          return currentSelector.getObject();
        } else {
          return null;
        }
      }

      @Override
      public boolean isNull()
      {
        refreshCursor();
        return !shouldEmitColumnValue(channel) || currentSelector.isNull();
      }

      @Override
      public Class<?> classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Not needed: TopN engine won't run on this.
        throw new UnsupportedOperationException();
      }

      private void refreshCursor()
      {
        final FrameCursor headCursor = trackers.get(channel).currentCursor();

        //noinspection ObjectEquality
        if (currentCursor != headCursor) {
          if (headCursor == null) {
            currentCursor = null;
            currentSelector = null;
          } else {
            currentCursor = headCursor;
            currentSelector = headCursor.getColumnSelectorFactory().makeColumnValueSelector(columnName);
          }
        }
      }
    }
  }

  private static class FrameHolder
  {
    private final Frame frame;
    private final RowKeyReader keyReader;
    private final FrameCursor cursor;
    private final FrameComparisonWidget comparisonWidget;

    public FrameHolder(Frame frame, RowKeyReader keyReader, FrameCursor cursor, FrameComparisonWidget comparisonWidget)
    {
      this.frame = frame;
      this.keyReader = keyReader;
      this.cursor = cursor;
      this.comparisonWidget = comparisonWidget;
    }
  }
}
