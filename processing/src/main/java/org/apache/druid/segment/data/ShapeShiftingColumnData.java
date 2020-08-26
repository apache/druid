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

package org.apache.druid.segment.data;

import it.unimi.dsi.fastutil.bytes.Byte2IntArrayMap;
import it.unimi.dsi.fastutil.bytes.Byte2IntMap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Materialized version of outer buffer contents of a {@link ShapeShiftingColumn}, extracting all header information
 * as well as a sliced buffer, prepared for reading, allowing suppliers a tidy structure to instantiate
 * {@link ShapeShiftingColumn} objects.
 *
 * layout:
 * | version (byte) | headerSize (int) | numValues (int) | numChunks (int) | logValuesPerChunk (byte) | offsetsSize (int) |  compositionSize (int) | composition | offsets | values |
 */
public class ShapeShiftingColumnData
{
  private final int headerSize;
  private final int numValues;
  private final int numChunks;
  private final byte logValuesPerChunk;
  private final byte logBytesPerValue;
  private final int compositionOffset;
  private final int compositionSize;
  private final int offsetsOffset;
  private final int offsetsSize;
  private final Byte2IntArrayMap composition;
  private final ChunkPosition[] chunkPositions;
  private final ByteBuffer baseBuffer;
  private final ByteOrder byteOrder;

  public ShapeShiftingColumnData(ByteBuffer buffer, byte logBytesPerValue, ByteOrder byteOrder)
  {
    this(buffer, logBytesPerValue, byteOrder, false);
  }

  public ShapeShiftingColumnData(
      ByteBuffer buffer,
      byte logBytesPerValue,
      ByteOrder byteOrder,
      boolean moveSourceBufferPosition
  )
  {
    ByteBuffer ourBuffer = buffer.slice().order(byteOrder);
    int position = 0;
    this.byteOrder = byteOrder;
    this.logBytesPerValue = logBytesPerValue;
    this.headerSize = ourBuffer.getInt(++position);
    this.numValues = ourBuffer.getInt(position += Integer.BYTES);
    this.numChunks = ourBuffer.getInt(position += Integer.BYTES);
    this.logValuesPerChunk = ourBuffer.get(position += Integer.BYTES);
    this.compositionOffset = ourBuffer.getInt(++position);
    this.compositionSize = ourBuffer.getInt(position += Integer.BYTES);
    this.offsetsOffset = ourBuffer.getInt(position += Integer.BYTES);
    this.offsetsSize = ourBuffer.getInt(position += Integer.BYTES);

    this.composition = new Byte2IntArrayMap();
    // 5 bytes per composition entry
    for (int i = 0; i < compositionSize; i += 5) {
      byte header = ourBuffer.get(compositionOffset + i);
      int count = ourBuffer.getInt(compositionOffset + i + 1);
      composition.put(header, count);
    }

    this.chunkPositions = new ChunkPosition[numChunks];
    int prevOffset = 0;
    for (int chunk = 0, chunkOffset = 0; chunk < numChunks; chunk++, chunkOffset += Integer.BYTES) {
      int chunkStart = ourBuffer.getInt(offsetsOffset + chunkOffset);
      int chunkEnd = ourBuffer.getInt(offsetsOffset + chunkOffset + Integer.BYTES);
      chunkPositions[chunk] = new ChunkPosition(chunkStart, chunkEnd);
      prevOffset = chunkEnd;
    }

    ourBuffer.limit(getValueChunksStartOffset() + prevOffset);

    if (moveSourceBufferPosition) {
      buffer.position(buffer.position() + ourBuffer.remaining());
    }

    this.baseBuffer = ourBuffer.slice().order(byteOrder);
  }

  /**
   * Total 'header' size, to future proof by allowing us to always be able to find offsets and values sections offsets,
   * but stuffing any additional data into the header.
   *
   * @return
   */
  public int getHeaderSize()
  {
    return headerSize;
  }

  /**
   * Total number of rows in this column
   *
   * @return
   */
  public int getNumValues()
  {
    return numValues;
  }

  /**
   * Number of 'chunks' of values this column is divided into
   *
   * @return
   */
  public int getNumChunks()
  {
    return numChunks;
  }

  /**
   * log base 2 max number of values per chunk
   *
   * @return
   */
  public byte getLogValuesPerChunk()
  {
    return logValuesPerChunk;
  }

  /**
   * log base 2 number of bytes per value
   *
   * @return
   */
  public byte getLogBytesPerValue()
  {
    return logBytesPerValue;
  }

  /**
   * Size in bytes of chunk offset data
   *
   * @return
   */
  public int getOffsetsSize()
  {
    return offsetsSize;
  }

  /**
   * Size in bytes of composition data
   *
   * @return
   */
  public int getCompositionSize()
  {
    return compositionSize;
  }

  /**
   * Get composition of codecs used in column and their counts, allowing column suppliers to optimize at query time.
   * Note that 'compressed' blocks are counted twice, once as compression and once as inner codec, so the total
   * count here may not match {@link ShapeShiftingColumnData#numChunks}
   *
   * @return
   */
  public Byte2IntMap getComposition()
  {
    return composition;
  }

  /**
   * get start and end offset of chunk in {@link ShapeShiftingColumnData#baseBuffer}
   * @param chunk index of chunk
   * @return
   */
  public ChunkPosition getChunkPosition(int chunk)
  {
    return chunkPositions[chunk];
  }

  /**
   * Start offset of {@link ShapeShiftingColumnData#baseBuffer} for the 'chunk values' section
   *
   * @return
   */
  public int getValueChunksStartOffset()
  {
    return headerSize;
  }

  /**
   * {@link ByteBuffer} View of column data, sliced from underlying mapped segment smoosh buffer.
   *
   * @return
   */
  public ByteBuffer getBaseBuffer()
  {
    return baseBuffer;
  }

  /**
   * Column byte order
   *
   * @return
   */
  public ByteOrder getByteOrder()
  {
    return byteOrder;
  }

  public static class ChunkPosition
  {
    private final int startOffset;
    private final int endOffset;

    public ChunkPosition(int startOffset, int endOffset)
    {
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    public int getStartOffset()
    {
      return startOffset;
    }

    public int getEndOffset()
    {
      return endOffset;
    }
  }
}
