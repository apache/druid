package io.druid.segment.data;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.io.ByteSink;
import com.google.common.io.CountingOutputStream;
import com.google.common.math.LongMath;
import com.google.common.primitives.Longs;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class RACompressionFormatSerde
{

  public static class RACompressedLongSupplierSerializer implements LongSupplierSerializer {

    private final IOPeon ioPeon;
    private final String filenameBase;
    private final String tempFile;
    private final ByteOrder order;
    private CountingOutputStream tempOut = null;

    private int numInserted = 0;

    private BiMap<Long, Integer> uniqueValues = HashBiMap.create();
    private long maxVal = 0;
    private long minVal = 0;

    private LongSupplierSerializer delegate;

    public RACompressedLongSupplierSerializer(
        IOPeon ioPeon,
        String filenameBase,
        ByteOrder order
    )
    {
      this.ioPeon = ioPeon;
      this.tempFile = filenameBase + ".temp";
      this.filenameBase = filenameBase;
      this.order = order;
    }

    public void open() throws IOException
    {
      tempOut = new CountingOutputStream(ioPeon.makeOutputStream(tempFile));
    }

    public int size()
    {
      return numInserted;
    }

    public void add(long value) throws IOException
    {
      tempOut.write(Longs.toByteArray(value));
      ++numInserted;
      if (uniqueValues.size() <= TableCompressionFormatSerde.MAX_TABLE_SIZE && !uniqueValues.containsKey(value)) {
        uniqueValues.put(value, uniqueValues.size());
      }
      if (value > maxVal) {
        maxVal = value;
      } else if (value < minVal) {
        minVal = value;
      }
    }

    private void makeDelegate() throws IOException
    {
      long delta;
      try {
        delta = LongMath.checkedSubtract(maxVal, minVal);
      } catch (ArithmeticException e) {
        delta = -1;
      }
      if (uniqueValues.size() <= TableCompressionFormatSerde.MAX_TABLE_SIZE) {
        delegate = new TableCompressionFormatSerde.TableCompressedLongSupplierSerializer(
            ioPeon, filenameBase, order, uniqueValues
        );
      } else if (delta != -1) {
        delegate = new DeltaCompressionFormatSerde.DeltaCompressedLongSupplierSerializer(
            ioPeon, filenameBase, order, minVal, delta
        );
      } else {
        delegate = new UncompressedFormatSerde.UncompressedLongSupplierSerializer(
            ioPeon, filenameBase, order
        );
      }

      DataInputStream tempIn = new DataInputStream(new BufferedInputStream(ioPeon.makeInputStream(tempFile)));
      delegate.open();
      while (tempIn.available() > 0) {
        delegate.add(tempIn.readLong());
      }
    }

    public void closeAndConsolidate(ByteSink consolidatedOut) throws IOException
    {
      tempOut.close();
      makeDelegate();
      delegate.closeAndConsolidate(consolidatedOut);
    }

    public void close() throws IOException {
      tempOut.close();
      makeDelegate();
      delegate.close();
    }

    public long getSerializedSize()
    {
      return delegate.getSerializedSize();
    }

    public void writeToChannel(WritableByteChannel channel) throws IOException
    {
      delegate.writeToChannel(channel);
    }
  }

}
