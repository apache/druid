package org.apache.druid.segment.data;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class FrontCodedIndexedWriter implements Serializer
{
  private static final NullableTypeStrategy<String> NULLABLE_STRING_STRATEGY = ColumnType.STRING.getNullableStrategy();
  // todo (clint): sir, just how big are your strings?
  private static final int MAX_LOG_BUFFER_SIZE = 16;

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final int bucketSize;
  private final ByteOrder byteOrder;

  @Nullable
  private String prevObject = null;
  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;

  private ByteBuffer scratch;
  private int logScratchSize = 10;
  private final String[] bucketBuffer;
  private boolean isClosed = false;
  private boolean hasNulls = false;


  public FrontCodedIndexedWriter(SegmentWriteOutMedium segmentWriteOutMedium, ByteOrder byteOrder, int bucketSize)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.scratch = ByteBuffer.allocate(1 << logScratchSize).order(byteOrder);
    this.bucketSize = bucketSize;
    this.bucketBuffer = new String[bucketSize];
    this.byteOrder = byteOrder;
  }

  private void resetScratch()
  {
    scratch.position(0);
    scratch.limit(scratch.capacity());
  }
  private void grow()
  {
    if (logScratchSize < MAX_LOG_BUFFER_SIZE) {
      this.scratch = ByteBuffer.allocate(1 << ++logScratchSize).order(byteOrder);
    } else {
      throw new IllegalStateException("scratch buffer to big to write buckets");
    }
  }

  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }


  public void write(@Nullable String objectToWrite) throws IOException
  {
    if (prevObject != null && NULLABLE_STRING_STRATEGY.compare(prevObject, objectToWrite) >= 0) {
      throw new ISE("Values must be sorted element [%s] with value [%s] is before [%s]", numWritten, objectToWrite, prevObject);
    }

    if (objectToWrite == null) {
      hasNulls = true;
      return;
    }

    if (numWritten > 0 && (numWritten) % bucketSize == 0) {
      resetScratch();
      int written;
      do {
        written = FrontCodedIndexed.writeBucket(scratch, bucketBuffer, bucketSize);
        if (written < 0) {
          grow();
        }
      } while (written < 0);
      scratch.flip();
      Channels.writeFully(valuesOut, scratch);

      resetScratch();
      scratch.putInt((int) valuesOut.size());
      scratch.flip();
      Channels.writeFully(headerOut, scratch);
    }

    bucketBuffer[numWritten % bucketSize] = objectToWrite;

    ++numWritten;
  }

  private void flush() throws IOException
  {
    int remainder = numWritten % bucketSize;
    resetScratch();
    int written;
    do {
      written = FrontCodedIndexed.writeBucket(scratch, bucketBuffer, remainder == 0 ? bucketSize : remainder);
      if (written < 0) {
        grow();
      }
    } while (written < 0);
    scratch.flip();
    Channels.writeFully(valuesOut, scratch);

    isClosed = true;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    if (!isClosed) {
      flush();
    }
    return Byte.BYTES + Byte.BYTES + FrontCodedIndexed.estimateSizeVByteInt(numWritten) + headerOut.size() + valuesOut.size();
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    if (!isClosed) {
      flush();
    }
    resetScratch();
    scratch.put((byte) bucketSize);
    scratch.put(hasNulls ? NullHandling.IS_NULL_BYTE : NullHandling.IS_NOT_NULL_BYTE);
    FrontCodedIndexed.writeVbyteInt(scratch, numWritten);
    scratch.flip();
    Channels.writeFully(channel, scratch);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }
}
