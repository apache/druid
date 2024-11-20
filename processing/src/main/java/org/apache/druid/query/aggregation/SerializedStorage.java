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

package org.apache.druid.query.aggregation;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.serde.cell.IOIterator;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.NoSuchElementException;

/**
 * simple utility class useful for when multiple passes of input are needed for encoding (e.g. delta or dictionary
 * encoding).
 * <p/>
 * This allows objects to be serialized to some temporary storage and iterated over for final processing.
 * <p/>
 * @param <T>
 */
public class SerializedStorage<T>
{
  private final WriteOutBytes writeOutBytes;
  private final StagedSerde<T> serde;
  private final ByteBuffer itemOffsetsBytes;
  private final IntBuffer itemSizes;

  private final LongArrayList rowChunkOffsets = new LongArrayList();
  private int numStored = 0;
  private int maxSize = 0;

  public SerializedStorage(WriteOutBytes writeOutBytes, StagedSerde<T> serde)
  {
    this(writeOutBytes, serde, 4096);
  }

  public SerializedStorage(WriteOutBytes writeOutBytes, StagedSerde<T> serde, int chunkSize)
  {
    this.writeOutBytes = writeOutBytes;
    this.serde = serde;

    this.itemOffsetsBytes = ByteBuffer.allocate(chunkSize * Integer.BYTES).order(ByteOrder.nativeOrder());
    this.itemSizes = itemOffsetsBytes.asIntBuffer();
  }

  public void store(@Nullable T value) throws IOException
  {
    byte[] bytes = serde.serialize(value);

    maxSize = Math.max(maxSize, bytes.length);
    itemSizes.put(bytes.length);
    if (bytes.length > 0) {
      writeOutBytes.write(bytes);
    }

    ++numStored;
    if (itemSizes.remaining() == 0) {
      rowChunkOffsets.add(writeOutBytes.size());
      writeOutBytes.write(itemOffsetsBytes);
      itemOffsetsBytes.clear();
      itemSizes.clear();
    }
  }

  public int numStored()
  {
    return numStored;
  }

  /**
   * Generates an iterator over everything that has been stored.  Also signifies the end of storing objects.
   * iterator() can be called multiple times if needed, but after iterator() is called, store() can no longer be
   * called.
   *
   * @return an iterator
   * @throws IOException on failure
   */
  public IOIterator<T> iterator() throws IOException
  {
    if (itemSizes.position() != itemSizes.limit()) {
      rowChunkOffsets.add(writeOutBytes.size());
      itemOffsetsBytes.limit(itemSizes.position() * Integer.BYTES);
      writeOutBytes.write(itemOffsetsBytes);

      // Move the limit to the position so that we fail subsequent writes and indicate that we are done
      itemSizes.limit(itemSizes.position());
    }

    return new DeserializingIOIterator<>(
        writeOutBytes,
        rowChunkOffsets,
        numStored,
        itemSizes.capacity(),
        maxSize,
        serde
    );
  }

  private static class DeserializingIOIterator<T> implements IOIterator<T>
  {
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final WriteOutBytes medium;
    private final LongArrayList rowChunkOffsets;
    private final int numEntries;
    private ByteBuffer tmpBuf;
    private final StagedSerde<T> serde;

    private final ByteBuffer itemOffsetsBytes;
    private final int[] itemSizes;

    private long itemStartOffset;
    private int chunkId = 0;
    private int currId = 0;
    private int itemIndex;

    public DeserializingIOIterator(
        WriteOutBytes medium,
        LongArrayList rowChunkOffsets,
        int numEntries,
        int chunkSize,
        int maxSize,
        StagedSerde<T> serde
    )
    {
      this.medium = medium;
      this.rowChunkOffsets = rowChunkOffsets;
      this.numEntries = numEntries;
      this.tmpBuf = ByteBuffer.allocate(maxSize).order(ByteOrder.nativeOrder());
      this.serde = serde;

      this.itemOffsetsBytes = ByteBuffer.allocate(chunkSize * Integer.BYTES).order(ByteOrder.nativeOrder());
      this.itemSizes = new int[chunkSize];
      this.itemIndex = chunkSize;
    }

    @Override
    public boolean hasNext()
    {
      return currId < numEntries;
    }

    @Override
    public T next() throws IOException
    {
      if (currId >= numEntries) {
        throw new NoSuchElementException();
      }

      if (itemIndex >= itemSizes.length) {
        if (chunkId == 0) {
          itemStartOffset = 0;
        } else {
          if (itemStartOffset != rowChunkOffsets.getLong(chunkId - 1)) {
            throw DruidException.defensive(
                "Should have read up to the start of the offsets [%,d], "
                + "but for some reason the values [%,d] don't align.  Possible corruption?",
                rowChunkOffsets.getLong(chunkId - 1),
                itemStartOffset
            );
          }
          itemStartOffset += (((long) itemSizes.length) * Integer.BYTES);
        }

        int numToRead = Math.min(itemSizes.length, numEntries - (chunkId * itemSizes.length));
        final long readOffset = rowChunkOffsets.getLong(chunkId++);
        itemOffsetsBytes.clear();
        itemOffsetsBytes.limit(numToRead * Integer.BYTES);
        medium.readFully(readOffset, itemOffsetsBytes);
        itemOffsetsBytes.flip();
        itemOffsetsBytes.asIntBuffer().get(itemSizes, 0, numToRead);

        itemIndex = 0;
      }

      int bytesToRead = itemSizes[itemIndex];
      final T retVal;
      if (bytesToRead == 0) {
        retVal = serde.deserialize(EMPTY_BUFFER);
      } else {
        tmpBuf.clear();
        tmpBuf.limit(bytesToRead);
        medium.readFully(itemStartOffset, tmpBuf);
        tmpBuf.flip();

        retVal = serde.deserialize(tmpBuf);
      }

      itemStartOffset += bytesToRead;
      ++itemIndex;
      ++currId;

      return retVal;
    }

    @Override
    public void close()
    {

    }
  }
}
