/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * Compression of metrics is done by using a combination of {@link io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy}
 * and {@link LongEncodingFormat}. CompressionStrategy is unaware of the data type and size and is based on bytes
 * operation. It must operate on blocks of bytes, and it must decompress the entire block to access the content.
 * EncodingFormat refers to any compression method that does not fit the description for CompressionStrategy.
 * <p>
 *
 * Compression Storage Format
 * Byte 1 : version (currently 0x02)
 * Byte 2 - 5 : number of values
 * Byte 6 - 9 : size per block (even if block format isn't used, this is needed for backward compatibility)
 * Byte 10 : compression strategy ( < -2 if there is an encoding format following, to get the strategy id add 126 to it)
 * Byte 11(optional) : encoding format
 * <p>
 * Encoding specific header (described below)
 * <p>
 * Block related header (if block compression is used, described in GenericIndexed)
 * <p>
 * Values
 */
public abstract class CompressionFactory
{

  public static final LongEncodingFormat DEFAULT_LONG_ENCODING = LongEncodingFormat.LONGS;

  /**
   * Delta Encoding Header v1:
   * Byte 1 : version
   * Byte 2 - 9 : base value
   * Byte 10 - 13 : number of bits per value
   */
  public static final byte DELTA_ENCODING_VERSION = 0x1;

  /**
   * Table Encoding Header v1 :
   * Byte 1 : version
   * Byte 2 - 5 : table size
   * Byte 6 - (6 + 8 * table size - 1) : table of encoding, where the ith 8-byte value is encoded as i
   */
  public static final byte TABLE_ENCODING_VERSION = 0x1;

  public static final int MAX_TABLE_SIZE = 256;

  /**
   * There is no header or version for Longs encoding format for backward compatibility
   */

  public enum LongEncodingFormat
  {
    /**
     * DELTA format encodes a series of longs by finding the smallest value first, and stores all values
     * as offset to the smallest value. The maximum value is also found to calculate how many bits are required
     * to store each offset using {@link VSizeLongSerde}.
     */
    DELTA((byte) 0x0) {
      @Override
      public LongEncodingFormatReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new DeltaEncodingReader(buffer);
      }

      @Override
      public LongEncodingFormatWriter getWriter(ByteOrder order)
      {
        return null;
      }
    },
    /**
     * TABLE format encodes a series of longs by mapping each unique value to an id, and string the id with the
     * minimum number of bits similar to how DELTA stores offset. TABLE format is only applicable to values with
     * less unique values than {@link CompressionFactory#MAX_TABLE_SIZE}.
     */
    TABLE((byte) 0x1) {
      @Override
      public LongEncodingFormatReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new TableEncodingReader(buffer);
      }

      @Override
      public LongEncodingFormatWriter getWriter(ByteOrder order)
      {
        return null;
      }
    },
    /**
     * LONGS format encodes longs as is, using 8 bytes for each value.
     */
    LONGS((byte) 0xFF) {
      @Override
      public LongEncodingFormatReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new LongsEncodingReader(buffer, order);
      }

      @Override
      public LongEncodingFormatWriter getWriter(ByteOrder order)
      {
        return new LongsEncodingWriter(order);
      }
    };

    final byte id;

    LongEncodingFormat(byte id)
    {
      this.id = id;
    }

    public byte getId()
    {
      return id;
    }

    static final Map<Byte, LongEncodingFormat> idMap = Maps.newHashMap();

    static {
      for (LongEncodingFormat format : LongEncodingFormat.values()) {
        idMap.put(format.getId(), format);
      }
    }

    public abstract LongEncodingFormatReader getReader(ByteBuffer buffer, ByteOrder order);

    public abstract LongEncodingFormatWriter getWriter(ByteOrder order);

    public static LongEncodingFormat forId(byte id)
    {
      return idMap.get(id);
    }
  }

  /**
   * This writer output encoded values to the given ByteBuffer or OutputStream. {@link #setBuffer(ByteBuffer)} or
   * {@link #setOutputStream(OutputStream)} must be called before any value is written, and {@link #flush()} must
   * be called before calling setBuffer or setOutputStream again to set another output.
   */
  public interface LongEncodingFormatWriter
  {
    /**
     * Data will be written starting from current position of the buffer, and the position of the buffer will be
     * updated as content is written.
     */
    void setBuffer(ByteBuffer buffer);

    void setOutputStream(OutputStream output);

    void write(long value) throws IOException;

    /**
     * Flush the unwritten content to the current output.
     */
    void flush() throws IOException;

    /**
     * Output the header values of the associating encoding format to the given outputStream. The header also include
     * bytes for compression strategy and encoding format(optional) as described above in Compression Storage Format.
     */
    void putMeta(OutputStream metaOut, CompressedObjectStrategy.CompressionStrategy strategy) throws IOException;

    /**
     * Get the number of values that can be encoded into each block for the given block size in bytes
     */
    int getBlockSize(int bytesPerBlock);

    /**
     * Get the number of bytes required to encoding the given number of values
     */
    int getNumBytes(int values);
  }

  public interface LongEncodingFormatReader
  {
    void setBuffer(ByteBuffer buffer);

    long read(int index);

    int getNumBytes(int values);

    LongEncodingFormatReader duplicate();
  }

  public static Supplier<IndexedLongs> getLongSupplier(
      int totalSize, int sizePer, ByteBuffer fromBuffer, ByteOrder order,
      LongEncodingFormat encoding,
      CompressedObjectStrategy.CompressionStrategy strategy
  )
  {
    if (strategy == CompressedObjectStrategy.CompressionStrategy.NONE) {
      return new EntireLayoutIndexedLongSupplier(totalSize, encoding.getReader(fromBuffer, order));
    } else {
      return new BlockLayoutIndexedLongsSupplier(totalSize, sizePer, fromBuffer, order,
                                                 encoding.getReader(fromBuffer, order), strategy
      );
    }
  }

  public static LongSupplierSerializer getLongSerializer(
      IOPeon ioPeon, String filenameBase, ByteOrder order,
      LongEncodingFormat encoding,
      CompressedObjectStrategy.CompressionStrategy strategy
  )
  {
    if (encoding == LongEncodingFormat.TABLE || encoding == LongEncodingFormat.DELTA) {
      return new IntermediateLongSupplierSerializer(ioPeon, filenameBase, order, strategy);
    }
    if (strategy == CompressedObjectStrategy.CompressionStrategy.NONE) {
      return new EntireLayoutLongSerializer(
          ioPeon, filenameBase, order, encoding.getWriter(order)
      );
    } else {
      return new BlockLayoutLongSupplierSerializer(
          ioPeon, filenameBase, order, encoding.getWriter(order), strategy
      );
    }
  }
}
