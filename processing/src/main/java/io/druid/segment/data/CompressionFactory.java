package io.druid.segment.data;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

/**
 * Format
 * Byte 1 : version (currently 0x02)
 * Byte 2 - 5 : number of values
 * Byte 6 - 9 : size per block (even if block format isn't used, this is needed for backward compatibility)
 * Byte 10 : compression strategy ( < -2 if there is an encoding format following, to get the strategy id add 126 to it)
 * Byte 11 : encoding format (optional)
 *
 * Encoding specific header (described in Long/Delta/TableEncodingFormatSerde)
 *
 * Block related header (if block compression is used, described in GenericIndexed)
 *
 * Values
 *
 */
public abstract class CompressionFactory
{

  public static final LongEncodingFormat DEFAULT_LONG_ENCODING = LongEncodingFormat.LONGS;

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
        return new DeltaEncodingFormatSerde.DeltaEncodingReader(buffer);
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
     * less unique values than {@link TableEncodingFormatSerde#MAX_TABLE_SIZE}.
     */
    TABLE((byte) 0x1) {
      @Override
      public LongEncodingFormatReader getReader(ByteBuffer buffer, ByteOrder order)
      {
        return new TableEncodingFormatSerde.TableEncodingReader(buffer);
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
        return new LongsEncodingFormatSerde.LongsEncodingReader(buffer, order);
      }

      @Override
      public LongEncodingFormatWriter getWriter(ByteOrder order)
      {
        return new LongsEncodingFormatSerde.LongsEncodingWriter(order);
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
   * This writer output encoded values to the given ByteBuffer or OutputStream. setBuffer or setOutputStream
   * must be called before any value is written, and close must be called before calling setBuffer or
   * setOutputStream again.
   */
  public interface LongEncodingFormatWriter
  {
    /**
     * Data will be written starting from current position of the buffer.
     * @param buffer
     */
    void setBuffer(ByteBuffer buffer);
    void setOutputStream(OutputStream output);
    void write(long value) throws IOException;
    /**
     * Close the writer, note this does not close the outstream or bytebuffer being written to
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Output the header values of the associating encoding format to the given outputStream. The header also
     * include bytes for compression strategy and encoding format(optional) as described above in Format.
     * @param metaOut
     * @param strategy
     * @throws IOException
     */
    void putMeta(OutputStream metaOut, CompressedObjectStrategy.CompressionStrategy strategy) throws IOException;
    int getBlockSize(int bytesPerBlock);
    int getNumBytes(int values);
  }

  public interface LongEncodingFormatReader
  {
    void setBuffer(ByteBuffer buffer);
    long read(int index);
    int numBytes(int values);
    LongEncodingFormatReader duplicate();
  }

  public static Supplier<IndexedLongs> getLongSupplier(int totalSize, int sizePer, ByteBuffer fromBuffer, ByteOrder order,
                                                       LongEncodingFormat format,
                                                       CompressedObjectStrategy.CompressionStrategy strategy)
  {
    if (strategy == CompressedObjectStrategy.CompressionStrategy.NONE) {
      return new EntireLayoutSerde.EntireLayoutIndexedLongSupplier(totalSize, format.getReader(fromBuffer, order));
    } else {
      return new BlockLayoutSerde.BlockLayoutIndexedLongsSupplier(totalSize, sizePer, fromBuffer, order,
                                                                  format.getReader(fromBuffer, order), strategy);
    }
  }

  public static LongSupplierSerializer getLongSerializer(IOPeon ioPeon, String filenameBase, ByteOrder order,
                                                         LongEncodingFormat format,
                                                         CompressedObjectStrategy.CompressionStrategy strategy)
  {
    if (format == LongEncodingFormat.TABLE || format == LongEncodingFormat.DELTA) {
      return new IntermediateSerde.IntermediateLongSupplierSerializer(ioPeon, filenameBase, order, strategy);
    }
    if (strategy == CompressedObjectStrategy.CompressionStrategy.NONE) {
      return new EntireLayoutSerde.EntireLayoutLongSerializer(
          ioPeon, filenameBase, order, format.getWriter(order)
      );
    } else {
      return new BlockLayoutSerde.BlockLayoutLongSupplierSerializer(
          ioPeon, filenameBase, order, format.getWriter(order), strategy
      );
    }
  }
}
