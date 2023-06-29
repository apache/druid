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

import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryRange;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.read.FrameReaderUtils;
import org.apache.druid.frame.segment.row.FrameColumnSelectorFactory;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Processor that hash-partitions rows from any number of input channels, and writes partitioned frames to output
 * channels.
 *
 * Input frames must be {@link FrameType#ROW_BASED}, and input signature must be the same as output signature.
 * This processor hashes each row using {@link Memory#xxHash64} with a seed of {@link #HASH_SEED}.
 */
public class FrameChannelHashPartitioner implements FrameProcessor<Long>
{
  private static final String PARTITION_COLUMN_NAME =
      StringUtils.format("%s_part", FrameWriterUtils.RESERVED_FIELD_PREFIX);
  private static final long HASH_SEED = 0;

  private final List<ReadableFrameChannel> inputChannels;
  private final List<WritableFrameChannel> outputChannels;
  private final FrameReader frameReader;
  private final int keyFieldCount;
  private final FrameWriterFactory frameWriterFactory;
  private final IntSet awaitSet;

  private Cursor cursor;
  private LongSupplier cursorRowPartitionNumberSupplier;
  private long rowsWritten;

  // Indirection allows FrameWriters to follow "cursor" even when it is replaced with a new instance.
  private final MultiColumnSelectorFactory cursorColumnSelectorFactory;
  private final FrameWriter[] frameWriters;

  public FrameChannelHashPartitioner(
      final List<ReadableFrameChannel> inputChannels,
      final List<WritableFrameChannel> outputChannels,
      final FrameReader frameReader,
      final int keyFieldCount,
      final FrameWriterFactory frameWriterFactory
  )
  {
    this.inputChannels = inputChannels;
    this.outputChannels = outputChannels;
    this.frameReader = frameReader;
    this.keyFieldCount = keyFieldCount;
    this.frameWriterFactory = frameWriterFactory;
    this.awaitSet = FrameProcessors.rangeSet(inputChannels.size());
    this.frameWriters = new FrameWriter[outputChannels.size()];
    this.cursorColumnSelectorFactory = new MultiColumnSelectorFactory(
        Collections.singletonList(() -> cursor.getColumnSelectorFactory()),
        frameReader.signature()
    ).withRowMemoryAndSignatureColumns();

    if (!frameReader.signature().equals(frameWriterFactory.signature())) {
      throw new IAE("Input signature does not match output signature");
    }
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return inputChannels;
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return outputChannels;
  }

  @Override
  public ReturnOrAwait<Long> runIncrementally(final IntSet readableInputs) throws IOException
  {
    if (cursor == null) {
      readNextFrame(readableInputs);
    }

    if (cursor != null) {
      processCursor();
    }

    if (cursor != null) {
      return ReturnOrAwait.runAgain();
    } else if (awaitSet.isEmpty()) {
      flushFrameWriters();
      return ReturnOrAwait.returnObject(rowsWritten);
    } else {
      return ReturnOrAwait.awaitAny(awaitSet);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(inputChannels(), outputChannels(), frameWriters);
  }

  private void processCursor() throws IOException
  {
    assert cursor != null;

    while (!cursor.isDone()) {
      final int partition = (int) cursorRowPartitionNumberSupplier.getAsLong();
      final FrameWriter frameWriter = getOrCreateFrameWriter(partition);

      if (frameWriter.addSelection()) {
        cursor.advance();
      } else if (frameWriter.getNumRows() > 0) {
        writeFrame(partition);
        return;
      } else {
        throw new FrameRowTooLargeException(frameWriterFactory.allocatorCapacity());
      }
    }

    cursor = null;
    cursorRowPartitionNumberSupplier = null;
  }

  private void readNextFrame(final IntSet readableInputs)
  {
    if (cursor != null) {
      throw new ISE("Already reading a frame");
    }

    final IntSet readySet = new IntAVLTreeSet(readableInputs);

    for (int channelNumber : readableInputs) {
      final ReadableFrameChannel channel = inputChannels.get(channelNumber);

      if (channel.isFinished()) {
        awaitSet.remove(channelNumber);
        readySet.remove(channelNumber);
      }
    }

    if (!readySet.isEmpty()) {
      // Read a random channel: avoid biasing towards lower-numbered channels.
      final int channelNumber = FrameProcessors.selectRandom(readySet);
      final ReadableFrameChannel channel = inputChannels.get(channelNumber);

      if (!channel.isFinished()) {
        // Need row-based frame so we can hash memory directly.
        final Frame frame = FrameType.ROW_BASED.ensureType(channel.read());

        final HashPartitionVirtualColumn hashPartitionVirtualColumn =
            new HashPartitionVirtualColumn(PARTITION_COLUMN_NAME, frameReader, keyFieldCount, outputChannels.size());

        cursor = FrameProcessors.makeCursor(
            frame,
            frameReader,
            VirtualColumns.create(Collections.singletonList(hashPartitionVirtualColumn))
        );

        cursorRowPartitionNumberSupplier =
            cursor.getColumnSelectorFactory().makeColumnValueSelector(PARTITION_COLUMN_NAME)::getLong;
      }
    }
  }

  private void flushFrameWriters() throws IOException
  {
    for (int i = 0; i < frameWriters.length; i++) {
      if (frameWriters[i] != null) {
        writeFrame(i);
      }
    }
  }

  private FrameWriter getOrCreateFrameWriter(final int partition)
  {
    if (frameWriters[partition] == null) {
      frameWriters[partition] = frameWriterFactory.newFrameWriter(cursorColumnSelectorFactory);
    }

    return frameWriters[partition];
  }

  private void writeFrame(final int partition) throws IOException
  {
    if (frameWriters[partition] == null || frameWriters[partition].getNumRows() == 0) {
      throw new ISE("Nothing to write for partition [%,d]", partition);
    }

    final Frame frame = Frame.wrap(frameWriters[partition].toByteArray());
    outputChannels.get(partition).write(frame);
    frameWriters[partition].close();
    frameWriters[partition] = null;
    rowsWritten += frame.numRows();
  }

  /**
   * Virtual column that provides a hash code of the {@link FrameType#ROW_BASED} frame row that is wrapped in
   * the provided {@link ColumnSelectorFactory}, using {@link FrameReaderUtils#makeRowMemorySupplier}.
   */
  private static class HashPartitionVirtualColumn implements VirtualColumn
  {
    private final String name;
    private final FrameReader frameReader;
    private final int keyFieldCount;
    private final int partitionCount;

    public HashPartitionVirtualColumn(
        final String name,
        final FrameReader frameReader,
        final int keyFieldCount,
        final int partitionCount
    )
    {
      this.name = name;
      this.frameReader = frameReader;
      this.keyFieldCount = keyFieldCount;
      this.partitionCount = partitionCount;
    }

    @Override
    public String getOutputName()
    {
      return name;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
    {
      final Supplier<MemoryRange<Memory>> rowMemorySupplier =
          FrameReaderUtils.makeRowMemorySupplier(factory, frameReader.signature());
      final int frameFieldCount = frameReader.signature().size();

      return new LongColumnSelector()
      {
        @Override
        public long getLong()
        {
          if (keyFieldCount == 0) {
            return 0;
          }

          final MemoryRange<Memory> rowMemoryRange = rowMemorySupplier.get();
          final Memory memory = rowMemoryRange.memory();
          final long keyStartPosition = (long) Integer.BYTES * frameFieldCount;
          final long keyEndPosition =
              memory.getInt(rowMemoryRange.start() + (long) Integer.BYTES * (keyFieldCount - 1));
          final int keyLength = (int) (keyEndPosition - keyStartPosition);
          final long hash = memory.xxHash64(rowMemoryRange.start() + keyStartPosition, keyLength, HASH_SEED);
          return Math.abs(hash % partitionCount);
        }

        @Override
        public boolean isNull()
        {
          return false;
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // Nothing to do.
        }
      };
    }

    @Override
    public ColumnCapabilities capabilities(String columnName)
    {
      return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG).setHasNulls(false);
    }

    @Override
    public List<String> requiredColumns()
    {
      return ImmutableList.of(
          FrameColumnSelectorFactory.ROW_MEMORY_COLUMN,
          FrameColumnSelectorFactory.ROW_SIGNATURE_COLUMN
      );
    }

    @Override
    public boolean usesDotNotation()
    {
      return false;
    }

    @Override
    public byte[] getCacheKey()
    {
      throw new UnsupportedOperationException();
    }
  }
}
