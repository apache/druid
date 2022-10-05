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

package org.apache.druid.segment.serde.cell;

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * <h2>usage:</h2>
 * <p/>
 * CellReader effectively stores a list of byte[] payloads that are retrievable randomly by index. The entirety of
 * the data is block compressed. For reading, see {@link CellReader}. Example usage:
 *<p/>
 * <pre>{@code
 *
 * StagedSerde<Fuu> fuuSerDe = new ...
 * // note that cellWriter.close() *must* be called before writeTo() in order to finalize the index
 * try (CellWriter cellWriter = new CellWriter.Builder(segmentWriteOutMedium).build()) {
 *
 *    fuuList.stream().map(fuuSerDe:serialize).forEach(cellWriter::write);
 *  }
 *  // at this point cellWriter contains the index and compressed data
 *
 *
 *  // transfers the index and compressed data in the format specified below. This method is idempotent and copies
 *  // the data each time.
 *  cellWriter.writeTo(writableChannel, fileSmoosher); // 2nd argument currently unused, may be null
 *
 * } </pre>
 * <p/>
 * Note that for use with CellReader, the contents written to the writableChannel must be available as a ByteBuffer
 * <p/>
 * <h2>Internal Storage Details</h2>
 * <p/>
 * <pre>
 * serialized data is of the form:
 *
 *    [cell index]
 *    [payload storage]
 *
 * each of these items is stored in compressed streams of blocks with a block index.
 *
 * A BlockCompressedPayloadWriter stores byte[] payloads. These may be accessed by creating a
 * BlockCompressedPayloadReader over the produced ByteBuffer. Reads may be done by giving a location in the
 * uncompressed stream and a size
 *
 * NOTE: {@link BlockCompressedPayloadBuffer} does not store nulls on write(). However, the cellIndex stores an entry
 * with a size of 0 for nulls and {@link CellReader} will return null for any null written
 *
 *  [blockIndexSize:int]
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * |      block index
 * |      compressed block # -> block start in compressed stream position (relative to data start)
 * |
 * |      0: [block position: int]
 * |      1: [block position: int]
 * |      ...
 * |      i: [block position: int]
 * |      ...
 * |      n: [block position: int]
 * |      n+1: [total compressed size ] // stored to simplify invariant of n+1 - n = length(n)
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * [dataSize:int]
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * | [compressed payload block 1]
 * | [compressed payload block 2]
 * | ...
 * | [compressed paylod block n]
 * |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
 * the CellIndexWriter stores an array of longs using the BlockCompressedPayloadWriter
 *
 * logically this an array of longs
 *
 * |    0: start_0 : long
 * |    1: start_1 : long
 * |    ...
 * |    n: start_n : long
 * |    n+1: start_n + length_n : long  //ie, next position that would have been written to
 * |                                   //used again for invariant of length_i = row_i+1 - row_i
 * |
 * |    but this will be stored as block compressed. Reads are done by addressing it as a long array of bytes
 * |
 * |    [block index size]
 * |    [block index>
 * |
 * |    [data stream size]
 * |    [block compressed payload stream]
 *
 * resulting in
 *
 * |    [cell index size]
 * | ----cell index------------------------
 * |    [block index size]
 * |    [block index]
 * |    [data stream size]
 * |    [block compressed payload stream]
 * | -------------------------------------
 * |    [data stream size]
 * | ----data stream------------------------
 * |    [block index size]
 * |    [block index]
 * |    [data stream size]
 * |    [block compressed payload stream]
 * | -------------------------------------
 * </pre>
 */

public class CellWriter implements Serializer, Closeable
{
  private final IntSerializer intSerializer = new IntSerializer();
  private final CellIndexWriter cellIndexWriter;
  private final BlockCompressedPayloadWriter payloadWriter;

  private CellWriter(CellIndexWriter cellIndexWriter, BlockCompressedPayloadWriter payloadWriter)
  {
    this.cellIndexWriter = cellIndexWriter;
    this.payloadWriter = payloadWriter;
  }

  public void write(byte[] cellBytes) throws IOException
  {
    if (cellBytes == null) {
      cellIndexWriter.persistAndIncrement(0);
    } else {
      cellIndexWriter.persistAndIncrement(cellBytes.length);
      payloadWriter.write(cellBytes);
    }
  }

  public void write(ByteBuffer cellByteBuffer) throws IOException
  {
    if (cellByteBuffer == null) {
      cellIndexWriter.persistAndIncrement(0);
    } else {
      cellIndexWriter.persistAndIncrement(cellByteBuffer.remaining());
      payloadWriter.write(cellByteBuffer);
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
  {
    channel.write(intSerializer.serialize(cellIndexWriter.getSerializedSize()));
    cellIndexWriter.writeTo(channel, smoosher);
    channel.write(intSerializer.serialize(payloadWriter.getSerializedSize()));
    payloadWriter.writeTo(channel, smoosher);
  }

  @Override
  public void close() throws IOException
  {
    cellIndexWriter.close();
    payloadWriter.close();
  }

  @Override
  public long getSerializedSize()
  {
    return intSerializer.getSerializedSize()
           + cellIndexWriter.getSerializedSize()
           + intSerializer.getSerializedSize()
           + payloadWriter.getSerializedSize();
  }

  public static Builder builder(SegmentWriteOutMedium segmentWriteOutMedium)
  {
    return new Builder(segmentWriteOutMedium);
  }

  public static class Builder
  {
    private final BlockCompressedPayloadWriter.Builder blockCompressedPayloadWriterBuilder;

    /**
     * Default instance with a {@link NativeClearedByteBufferProvider}
     *
     * @param segmentWriteOutMedium - used store block-compressed index and data
     */
    public Builder(SegmentWriteOutMedium segmentWriteOutMedium)
    {
      blockCompressedPayloadWriterBuilder =
          new BlockCompressedPayloadWriter.Builder(segmentWriteOutMedium);
    }

    /**
     * change the compression strategy. The default is LZ4
     *
     * @param compressionStrategy - a valid {@link CompressionStrategy}
     * @return
     */
    public Builder setCompressionStrategy(CompressionStrategy compressionStrategy)
    {
      blockCompressedPayloadWriterBuilder.setCompressionStrategy(compressionStrategy);

      return this;
    }

    public Builder setByteBufferProvider(ByteBufferProvider byteBufferProvider)
    {
      blockCompressedPayloadWriterBuilder.setByteBufferProvider(byteBufferProvider);

      return this;
    }

    public CellWriter build() throws IOException
    {
      BlockCompressedPayloadWriter cellIndexPayloadWriter = blockCompressedPayloadWriterBuilder.build();
      BlockCompressedPayloadWriter payloadWriter = blockCompressedPayloadWriterBuilder.build();
      CellIndexWriter cellIndexWriter = new CellIndexWriter(cellIndexPayloadWriter);

      return new CellWriter(cellIndexWriter, payloadWriter);
    }
  }
}
