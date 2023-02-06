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

package org.apache.druid.msq.indexing;

import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.FrameComparisonWidget;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessors;
import org.apache.druid.frame.processor.ReturnOrAwait;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

public class KeyStatisticsCollectionProcessor implements FrameProcessor<ClusterByStatisticsCollector>
{
  /**
   * Constant chosen such that a column full of "standard" values, with row count
   * {@link MSQTaskQueryMaker#DEFAULT_ROWS_PER_SEGMENT}, and *some* redundancy between
   * rows (therefore: some "reasonable" compression) will not have any columns greater than 2GB in size.
   */
  private static final int STANDARD_VALUE_SIZE = 1000;

  /**
   * Constant chosen such that a segment full of "standard" rows, with row count
   * {@link MSQTaskQueryMaker#DEFAULT_ROWS_PER_SEGMENT}, and *some* redundancy between
   * rows (therefore: some "reasonable" compression) will not be larger than 5GB in size.
   */
  private static final int STANDARD_ROW_SIZE = 2000;

  private final ReadableFrameChannel inputChannel;
  private final WritableFrameChannel outputChannel;
  private final FrameReader frameReader;
  private final ClusterBy clusterBy;

  private ClusterByStatisticsCollector clusterByStatisticsCollector;

  public KeyStatisticsCollectionProcessor(
      final ReadableFrameChannel inputChannel,
      final WritableFrameChannel outputChannel,
      final FrameReader frameReader,
      final ClusterBy clusterBy,
      final ClusterByStatisticsCollector clusterByStatisticsCollector
  )
  {
    this.inputChannel = inputChannel;
    this.outputChannel = outputChannel;
    this.frameReader = frameReader;
    this.clusterBy = clusterBy;
    this.clusterByStatisticsCollector = clusterByStatisticsCollector;
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
  public ReturnOrAwait<ClusterByStatisticsCollector> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inputChannel.isFinished()) {
      return ReturnOrAwait.returnObject(clusterByStatisticsCollector);
    }

    final Frame frame = inputChannel.read();
    final Cursor cursor = FrameProcessors.makeCursor(frame, frameReader);
    final IntSupplier rowWeightSupplier = makeRowWeightSupplier(frameReader, cursor.getColumnSelectorFactory());
    final FrameComparisonWidget comparisonWidget = frameReader.makeComparisonWidget(frame, clusterBy.getColumns());

    for (int i = 0; i < frame.numRows(); i++, cursor.advance()) {
      final RowKey key = comparisonWidget.readKey(i);
      clusterByStatisticsCollector.add(key, rowWeightSupplier.getAsInt());
    }

    // Clears partition info (uses NO_PARTITION), but that's OK, because it isn't needed downstream of this processor.
    outputChannel.write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
    return ReturnOrAwait.awaitAll(1);
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(
        inputChannels(),
        outputChannels(),
        () -> clusterByStatisticsCollector = null
    );
  }

  private IntSupplier makeRowWeightSupplier(
      final FrameReader frameReader,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    final Supplier<MemoryRange<Memory>> rowMemorySupplier =
        FrameReaderUtils.makeRowMemorySupplier(columnSelectorFactory, frameReader.signature());

    final int numFields = frameReader.signature().size();

    if (rowMemorySupplier == null) {
      // Can't access row memory.
      throw new ISE("Can't read row memory from frame. Wrong frame type or signature?");
    }

    return () -> {
      final MemoryRange<Memory> rowMemory = rowMemorySupplier.get();

      if (rowMemory == null) {
        // Can't access row memory.
        throw new ISE("Can't read row memory from frame. Wrong type or signature?");
      }

      long maxValueLength = 0;
      long totalLength = 0;
      long currentValueStartPosition = (long) Integer.BYTES * numFields;

      for (int i = 0; i < numFields; i++) {
        final long currentValueEndPosition = rowMemory.memory().getInt(rowMemory.start() + (long) Integer.BYTES * i);
        final long valueLength = currentValueEndPosition - currentValueStartPosition;

        if (valueLength > maxValueLength) {
          maxValueLength = valueLength;
        }

        totalLength += valueLength;
        currentValueStartPosition = currentValueEndPosition;
      }

      return 1 + Ints.checkedCast(
          Math.max(
              maxValueLength / STANDARD_VALUE_SIZE,
              totalLength / STANDARD_ROW_SIZE
          )
      );
    };
  }
}
