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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


/**
 * Streams arrays of objects out in the binary format described by {@link GenericIndexed}
 */
public class GenericIndexedWriter<T> implements Serializer
{
  private static final int PAGE_SIZE = 4096;

  private static final MetaSerdeHelper<GenericIndexedWriter> SINGLE_FILE_META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((GenericIndexedWriter x) -> GenericIndexed.VERSION_ONE)
      .writeByte(
          x -> x.objectsSorted ? GenericIndexed.REVERSE_LOOKUP_ALLOWED : GenericIndexed.REVERSE_LOOKUP_DISALLOWED
      )
      .writeInt(x -> Ints.checkedCast(x.headerOut.size() + x.valuesOut.size() + Integer.BYTES))
      .writeInt(x -> x.numWritten);

  private static final MetaSerdeHelper<GenericIndexedWriter> MULTI_FILE_META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((GenericIndexedWriter x) -> GenericIndexed.VERSION_TWO)
      .writeByte(
          x -> x.objectsSorted ? GenericIndexed.REVERSE_LOOKUP_ALLOWED : GenericIndexed.REVERSE_LOOKUP_DISALLOWED
      )
      .writeInt(GenericIndexedWriter::bagSizePower)
      .writeInt(x -> x.numWritten)
      .writeInt(x -> x.fileNameByteArray.length)
      .writeByteArray(x -> x.fileNameByteArray);


  static GenericIndexedWriter<ByteBuffer> ofCompressedByteBuffers(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final String filenameBase,
      final CompressionStrategy compressionStrategy,
      final int bufferSize
  )
  {
    GenericIndexedWriter<ByteBuffer> writer = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        filenameBase,
        compressedByteBuffersWriteObjectStrategy(compressionStrategy, bufferSize, segmentWriteOutMedium.getCloser())
    );
    writer.objectsSorted = false;
    return writer;
  }

  static ObjectStrategy<ByteBuffer> compressedByteBuffersWriteObjectStrategy(
      final CompressionStrategy compressionStrategy,
      final int bufferSize,
      final Closer closer
  )
  {
    return new ObjectStrategy<ByteBuffer>()
    {
      private final CompressionStrategy.Compressor compressor = compressionStrategy.getCompressor();
      private final ByteBuffer compressedDataBuffer = compressor.allocateOutBuffer(bufferSize, closer);

      @Override
      public Class<ByteBuffer> getClazz()
      {
        return ByteBuffer.class;
      }

      @Override
      public ByteBuffer fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] toBytes(ByteBuffer val)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void writeTo(ByteBuffer val, WriteOutBytes out) throws IOException
      {
        compressedDataBuffer.clear();
        int valPos = val.position();
        out.write(compressor.compress(val, compressedDataBuffer));
        val.position(valPos);
      }

      @Override
      public int compare(ByteBuffer o1, ByteBuffer o2)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;
  private final int fileSizeLimit;
  private final byte[] fileNameByteArray;
  private boolean objectsSorted = true;
  @Nullable
  private T prevObject = null;
  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;
  private boolean requireMultipleFiles = false;
  @Nullable
  private LongList headerOutLong;

  // Used by checkedCastNonnegativeLongToInt. Will always be Integer.MAX_VALUE in production.
  private int intMaxForCasting = Integer.MAX_VALUE;

  private final ByteBuffer getOffsetBuffer = ByteBuffer.allocate(Integer.BYTES);

  public GenericIndexedWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this(segmentWriteOutMedium, filenameBase, strategy, Integer.MAX_VALUE & ~PAGE_SIZE);
  }

  public GenericIndexedWriter(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase,
      ObjectStrategy<T> strategy,
      int fileSizeLimit
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.fileSizeLimit = fileSizeLimit;
    fileNameByteArray = StringUtils.toUtf8(filenameBase);
  }

  public static String generateValueFileName(String fileNameBase, int fileNum)
  {
    return StringUtils.format("%s_value_%d", fileNameBase, fileNum);
  }

  public static String generateHeaderFileName(String fileNameBase)
  {
    return StringUtils.format("%s_header", fileNameBase);
  }

  private static void writeBytesIntoSmooshedChannel(
      long numBytesToPutInFile,
      final byte[] buffer,
      final SmooshedWriter smooshChannel,
      final InputStream is
  )
      throws IOException
  {
    ByteBuffer holderBuffer = ByteBuffer.wrap(buffer);
    while (numBytesToPutInFile > 0) {
      int bytesRead = is.read(buffer, 0, Math.min(buffer.length, Ints.saturatedCast(numBytesToPutInFile)));
      if (bytesRead != -1) {
        smooshChannel.write((ByteBuffer) holderBuffer.clear().limit(bytesRead));
        numBytesToPutInFile -= bytesRead;
      } else {
        throw new ISE("Could not write [%d] bytes into smooshChannel.", numBytesToPutInFile);
      }
    }
  }

  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  public void setObjectsNotSorted()
  {
    objectsSorted = false;
  }

  @VisibleForTesting
  void setIntMaxForCasting(final int intMaxForCasting)
  {
    this.intMaxForCasting = intMaxForCasting;
  }

  public void write(@Nullable T objectToWrite) throws IOException
  {
    if (objectsSorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      objectsSorted = false;
    }

    // for compatibility with the format (see GenericIndexed javadoc for description of the format),
    // this field is used to store nullness marker, but in a better format this info can take 1 bit.
    valuesOut.writeInt(objectToWrite == null ? GenericIndexed.NULL_VALUE_SIZE_MARKER : 0);
    if (objectToWrite != null) {
      strategy.writeTo(objectToWrite, valuesOut);
    }

    // Before updating the header, check if we need to switch to multi-file mode.
    if (!requireMultipleFiles && getSerializedSize() > fileSizeLimit) {
      requireMultipleFiles = true;
      initializeHeaderOutLong();
    }

    // Increment number of values written. Important to do this after the check above, since numWritten is
    // accessed during "initializeHeaderOutLong" to determine the length of the header.
    ++numWritten;

    if (!requireMultipleFiles) {
      headerOut.writeInt(checkedCastNonnegativeLongToInt(valuesOut.size()));

      // Check _again_ if we need to switch to multi-file mode. (We might need to after updating the header.)
      if (getSerializedSize() > fileSizeLimit) {
        requireMultipleFiles = true;
        initializeHeaderOutLong();
      }
    } else {
      headerOutLong.add(valuesOut.size());
    }

    if (objectsSorted) {
      prevObject = objectToWrite;
    }
  }

  @Nullable
  public T get(int index) throws IOException
  {
    long startOffset;
    if (index == 0) {
      startOffset = Integer.BYTES;
    } else {
      startOffset = getOffset(index - 1) + Integer.BYTES;
    }
    long endOffset = getOffset(index);
    int valueSize = checkedCastNonnegativeLongToInt(endOffset - startOffset);
    if (valueSize == 0) {
      return null;
    }
    ByteBuffer bb = ByteBuffer.allocate(valueSize);
    valuesOut.readFully(startOffset, bb);
    bb.clear();
    return strategy.fromByteBuffer(bb, valueSize);
  }

  private long getOffset(int index) throws IOException
  {
    if (!requireMultipleFiles) {
      getOffsetBuffer.clear();
      headerOut.readFully(index * (long) Integer.BYTES, getOffsetBuffer);
      return getOffsetBuffer.getInt(0);
    } else {
      return headerOutLong.getLong(index);
    }
  }

  @Override
  public long getSerializedSize()
  {
    if (requireMultipleFiles) {
      // for multi-file version (version 2), getSerializedSize() returns number of bytes in meta file.
      return MULTI_FILE_META_SERDE_HELPER.size(this);
    } else {
      return SINGLE_FILE_META_SERDE_HELPER.size(this) + headerOut.size() + valuesOut.size();
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
  {
    if (requireMultipleFiles) {
      writeToMultiFiles(channel, smoosher);
    } else {
      writeToSingleFile(channel);
    }
  }

  private void writeToSingleFile(WritableByteChannel channel) throws IOException
  {
    final long numBytesWritten = headerOut.size() + valuesOut.size();

    Preconditions.checkState(
        headerOut.size() == (numWritten * 4L),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4L,
        headerOut.size()
    );
    Preconditions.checkState(
        numBytesWritten < fileSizeLimit, "Wrote[%s] bytes, which is too many.",
        numBytesWritten
    );

    SINGLE_FILE_META_SERDE_HELPER.writeTo(channel, this);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  private void writeToMultiFiles(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(
        headerOutLong.size() == numWritten,
        "numWritten[%s] number of rows doesn't match headerOutLong's size[%s]",
        numWritten,
        headerOutLong.size()
    );
    Preconditions.checkState(
        (((long) headerOutLong.size()) * Long.BYTES) < (Integer.MAX_VALUE & ~PAGE_SIZE),
        "Wrote[%s] bytes in header, which is too many.",
        (((long) headerOutLong.size()) * Long.BYTES)
    );

    if (smoosher == null) {
      throw new IAE("version 2 GenericIndexedWriter requires FileSmoosher.");
    }

    int bagSizePower = bagSizePower();
    MULTI_FILE_META_SERDE_HELPER.writeTo(channel, this);

    long previousValuePosition = 0;
    int bagSize = 1 << bagSizePower;

    int numberOfFilesRequired = GenericIndexed.getNumberOfFilesRequired(bagSize, numWritten);
    byte[] buffer = new byte[1 << 16];

    try (InputStream is = valuesOut.asInputStream()) {
      int counter = -1;
      for (int i = 0; i < numberOfFilesRequired; i++) {
        long valuePosition;
        if (i != numberOfFilesRequired - 1) {
          valuePosition = headerOutLong.getLong(bagSize + counter);
          counter = counter + bagSize;
        } else {
          valuePosition = headerOutLong.getLong(numWritten - 1);
        }

        long numBytesToPutInFile = valuePosition - previousValuePosition;

        try (SmooshedWriter smooshChannel = smoosher
            .addWithSmooshedWriter(generateValueFileName(filenameBase, i), numBytesToPutInFile)) {
          writeBytesIntoSmooshedChannel(numBytesToPutInFile, buffer, smooshChannel, is);
          previousValuePosition = valuePosition;
        }
      }
    }
    writeHeaderLong(smoosher, bagSizePower);
  }

  /**
   * Tries to get best value split(number of elements in each value file) which can be expressed as power of 2.
   *
   * @return Returns the size of value file splits as power of 2.
   *
   * @throws IOException
   */
  private int bagSizePower()
  {
    long avgObjectSize = (valuesOut.size() + numWritten - 1) / numWritten;

    for (int i = 31; i >= 0; --i) {
      if ((1L << i) * avgObjectSize <= fileSizeLimit) {
        if (actuallyFits(i)) {
          return i;
        }
      }
    }
    throw new ISE(
        "no value split found with fileSizeLimit [%d], avgObjectSize [%d]",
        fileSizeLimit,
        avgObjectSize
    );
  }

  /**
   * Checks if candidate value splits can divide value file in such a way no object/element crosses the value splits.
   *
   * @param powerTwo candidate value split expressed as power of 2.
   *
   * @return true if candidate value split can hold all splits.
   *
   * @throws IOException
   */
  private boolean actuallyFits(int powerTwo)
  {
    long lastValueOffset = 0;
    long currentValueOffset = 0;
    long valueBytesWritten = valuesOut.size();
    long headerIndex = 0;
    long bagSize = 1L << powerTwo;

    while (lastValueOffset < valueBytesWritten) {

      if (headerIndex >= numWritten) {
        return true;
      } else if (headerIndex + bagSize <= numWritten) {
        currentValueOffset = headerOutLong.getLong(checkedCastNonnegativeLongToInt(headerIndex + bagSize - 1));
      } else if (numWritten < headerIndex + bagSize) {
        currentValueOffset = headerOutLong.getLong(numWritten - 1);
      }

      if (currentValueOffset - lastValueOffset <= fileSizeLimit) {
        lastValueOffset = currentValueOffset;
        headerIndex = headerIndex + bagSize;
      } else {
        return false;
      }
    }
    return true;
  }

  private void writeHeaderLong(FileSmoosher smoosher, int bagSizePower)
      throws IOException
  {
    ByteBuffer helperBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());

    int numberOfElementsPerValueFile = 1 << bagSizePower;
    long currentNumBytes = 0;
    long relativeRefBytes = 0;
    long relativeNumBytes;
    try (SmooshedWriter smooshChannel = smoosher
        .addWithSmooshedWriter(generateHeaderFileName(filenameBase), ((long) numWritten) * Integer.BYTES)) {

      // following block converts long header indexes into int header indexes.
      for (int pos = 0; pos < numWritten; pos++) {
        //conversion of header offset from long to int completed for one value file done, change relativeRefBytes
        // to current offset.
        if ((pos & (numberOfElementsPerValueFile - 1)) == 0) {
          relativeRefBytes = currentNumBytes;
        }
        currentNumBytes = headerOutLong.getLong(pos);
        relativeNumBytes = currentNumBytes - relativeRefBytes;
        helperBuffer.putInt(0, checkedCastNonnegativeLongToInt(relativeNumBytes));
        helperBuffer.clear();
        smooshChannel.write(helperBuffer);
      }
    }
  }

  private void initializeHeaderOutLong() throws IOException
  {
    headerOutLong = new LongArrayList();
    try (final DataInputStream headerOutAsIntInput = new DataInputStream(headerOut.asInputStream())) {
      for (int i = 0; i < numWritten; i++) {
        int count = headerOutAsIntInput.readInt();
        headerOutLong.add(count);
      }
    }
  }

  /**
   * Cast a long to an int, throwing an exception if it is out of range. Uses "intMaxForCasting" as the max
   * integer value. Only works for nonnegative "n".
   */
  private int checkedCastNonnegativeLongToInt(final long n)
  {
    if (n >= 0 && n <= intMaxForCasting) {
      return (int) n;
    } else {
      // Likely bug.
      throw new IAE("Value out of nonnegative int range");
    }
  }
}
