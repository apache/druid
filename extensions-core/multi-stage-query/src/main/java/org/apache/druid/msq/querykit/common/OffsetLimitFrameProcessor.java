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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.Cursor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class OffsetLimitFrameProcessor implements FrameProcessor<Long>
{
  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final long offset;
  private final long limit;

  long rowsProcessedSoFar = 0L;

  OffsetLimitFrameProcessor(
      ReadableFrameChannel inputChannel,
      WritableFrameChannel outputChannel,
      FrameReader frameReader,
      long offset,
      long limit
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.offset = offset;
    this.limit = limit;

    if (offset < 0 || limit < 0) {
      throw new ISE("Offset and limit must be nonnegative");
    }
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inputChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.singletonList(outputChannel);
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    } else if (inputChannel.isFinished() || rowsProcessedSoFar == offset + limit) {
      return ReturnOrAwait.returnObject(rowsProcessedSoFar);
    }

    final Frame frame = inputChannel.read();
    final Frame truncatedFrame = chopAndProcess(frame, frameReader);

    if (truncatedFrame != null) {
      outputChannel.write(new FrameWithPartition(truncatedFrame, FrameWithPartition.NO_PARTITION));
    }

    if (rowsProcessedSoFar == offset + limit) {
      // This check is not strictly necessary, given the check above, but prevents one extra scheduling round.
      return ReturnOrAwait.returnObject(rowsProcessedSoFar);
    } else {
      assert rowsProcessedSoFar < offset + limit;
      return ReturnOrAwait.awaitAll(1);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels());
  }

  /**
   * Chops a frame down to a smaller one, potentially on both ends.
   *
   * Increments {@link #rowsProcessedSoFar} as it does its work. Either returns the original frame, a chopped frame,
   * or null if no rows from the current frame should be included.
   */
  @Nullable
  private Frame chopAndProcess(final Frame frame, final FrameReader frameReader)
  {
    final long startRow = Math.max(0, offset - rowsProcessedSoFar);
    final long endRow = Math.min(frame.numRows(), offset + limit - rowsProcessedSoFar);

    if (startRow >= endRow) {
      // Offset is past the end of the frame; skip it.
      rowsProcessedSoFar += frame.numRows();
      return null;
    } else if (startRow == 0 && endRow == frame.numRows()) {
      rowsProcessedSoFar += frame.numRows();
      return frame;
    }

    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);

    // Using an unlimited memory allocator to make sure that atleast a single frame can always be generated
    final HeapMemoryAllocator unlimitedAllocator = HeapMemoryAllocator.unlimited();

    long rowsProcessedSoFarInFrame = 0;

    final FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        unlimitedAllocator,
        frameReader.signature(),
        Collections.emptyList()
    );

    try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(cursor.getColumnSelectorFactory())) {
      while (!cursor.isDone() && rowsProcessedSoFarInFrame < endRow) {
        if (rowsProcessedSoFarInFrame >= startRow && !frameWriter.addSelection()) {
          // Don't retry; it can't work because the allocator is unlimited anyway.
          // Also, I don't think this line can be reached, because the allocator is unlimited.
          throw new FrameRowTooLargeException(unlimitedAllocator.capacity());
        }

        cursor.advance();
        rowsProcessedSoFarInFrame++;
      }

      rowsProcessedSoFar += rowsProcessedSoFarInFrame;
      return Frame.wrap(frameWriter.toByteArray());
    }
  }
}
