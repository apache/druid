package io.druid.segment.data;

import com.google.common.collect.BiMap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Storage Format v1 :
 * Byte 1 : version
 * Byte 2 - 5 : table size
 * Byte 6 - (6 + 8 * table size - 1) : table of encoding, where the ith 8-byte value is encoded as i
 */
public class TableEncodingFormatSerde
{

  public static final int MAX_TABLE_SIZE = 256;

  private static final byte V1 = 0x1;

  public static class TableEncodingReader implements CompressionFactory.LongEncodingFormatReader
  {
    private final long table[];
    private final int bitsPerValue;
    private final ByteBuffer buffer;
    private VSizeLongSerde.LongDeserializer deserializer;

    public TableEncodingReader(ByteBuffer fromBuffer) {
      this.buffer = fromBuffer.asReadOnlyBuffer();
      byte version = buffer.get();
      if (version == V1) {
        int tableSize = buffer.getInt();
        if (tableSize < 0 || tableSize > MAX_TABLE_SIZE) {
          throw new IAE("Invalid table size[%s]", tableSize);
        }
        bitsPerValue = VSizeLongSerde.getBitsForMax(tableSize);
        table = new long[tableSize];
        for (int i = 0; i < tableSize; i++) {
          table[i] = buffer.getLong();
        }
        fromBuffer.position(buffer.position());
        deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
      } else {
        throw new IAE("Unknown version[%s]", version);
      }
    }

    public TableEncodingReader(ByteBuffer buffer, long table[], int bitsPerValue) {
      this.buffer = buffer.asReadOnlyBuffer();
      this.table = table;
      this.bitsPerValue = bitsPerValue;
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    }

    @Override
    public void setBuffer(ByteBuffer buffer)
    {
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    }

    @Override
    public long read(int index)
    {
      return table[(int)deserializer.get(index)];
    }

    @Override
    public int numBytes(int values)
    {
      return VSizeLongSerde.getSerializedSize(bitsPerValue, values);
    }

    @Override
    public CompressionFactory.LongEncodingFormatReader duplicate()
    {
      return new TableEncodingReader(buffer, table, bitsPerValue);
    }
  }

  public static class TableEncodingWriter implements CompressionFactory.LongEncodingFormatWriter
  {

    private final BiMap<Long, Integer> table;
    private final int bitsPerValue;
    private VSizeLongSerde.LongSerializer serializer;

    public TableEncodingWriter(BiMap<Long, Integer> table) {
      if (table.size() > MAX_TABLE_SIZE) {
        throw new IAE("Invalid table size[%s]", table.size());
      }
      this.table = table;
      this.bitsPerValue = VSizeLongSerde.getBitsForMax(table.size());
    }

    @Override
    public void setBuffer(ByteBuffer buffer)
    {
      serializer = VSizeLongSerde.getSerializer(bitsPerValue, buffer, buffer.position());
    }

    @Override
    public void setOutputStream(OutputStream output)
    {
      serializer = VSizeLongSerde.getSerializer(bitsPerValue, output);
    }

    @Override
    public void write(long value) throws IOException
    {
      serializer.write(table.get(value));
    }

    @Override
    public void close() throws IOException
    {
      if (serializer != null) {
        serializer.close();
      }
    }

    @Override
    public void putMeta(OutputStream metaOut, CompressedObjectStrategy.CompressionStrategy strategy) throws IOException
    {
      metaOut.write(strategy.getId() - 126);
      metaOut.write(CompressionFactory.LongEncodingFormat.TABLE.getId());
      metaOut.write(V1);
      metaOut.write(Ints.toByteArray(table.size()));
      BiMap<Integer, Long> inverse = table.inverse();
      for (int i = 0; i < table.size() ; i++) {
        metaOut.write(Longs.toByteArray(inverse.get(i)));
      }
    }

    @Override
    public int getBlockSize(int bytesPerBlock)
    {
      return VSizeLongSerde.getBlockSize(bitsPerValue, bytesPerBlock);
    }

    @Override
    public int getNumBytes(int values)
    {
      return VSizeLongSerde.getSerializedSize(bitsPerValue, values);
    }
  }
}
