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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.io.Channels;
import io.druid.io.OutputBytes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;
import io.druid.segment.serde.Serializer;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;


/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements Serializer
{
  private static int PAGE_SIZE = 4096;

  static GenericIndexedWriter<ByteBuffer> ofCompressedByteBuffers(
      final String filenameBase,
      final CompressionStrategy compressionStrategy,
      final int bufferSize
  )
  {
    GenericIndexedWriter<ByteBuffer> writer = new GenericIndexedWriter<>(
        filenameBase,
        compressedByteBuffersWriteObjectStrategy(compressionStrategy, bufferSize)
    );
    writer.objectsSorted = false;
    return writer;
  }

  static ObjectStrategy<ByteBuffer> compressedByteBuffersWriteObjectStrategy(
      final CompressionStrategy compressionStrategy,
      final int bufferSize
  )
  {
    return new ObjectStrategy<ByteBuffer>()
    {
      private final CompressionStrategy.Compressor compressor = compressionStrategy.getCompressor();
      private final ByteBuffer compressedDataBuffer = compressor.allocateOutBuffer(bufferSize);

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
      public void writeTo(ByteBuffer val, OutputBytes out) throws IOException
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


  private final String filenameBase;
  private final ObjectStrategy<T> strategy;
  private final int fileSizeLimit;
  private final byte[] fileNameByteArray;
  private boolean objectsSorted = true;
  private T prevObject = null;
  private OutputBytes headerOut = null;
  private OutputBytes valuesOut = null;
  private int numWritten = 0;
  private boolean requireMultipleFiles = false;
  private LongList headerOutLong;

  public GenericIndexedWriter(String filenameBase, ObjectStrategy<T> strategy)
  {
    this(filenameBase, strategy, Integer.MAX_VALUE & ~PAGE_SIZE);
  }

  public GenericIndexedWriter(String filenameBase, ObjectStrategy<T> strategy, int fileSizeLimit)
  {
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.fileSizeLimit = fileSizeLimit;
    fileNameByteArray = filenameBase.getBytes();
  }

  public static String generateValueFileName(String fileNameBase, int fileNum)
  {
    return String.format("%s_value_%d", fileNameBase, fileNum);
  }

  public static String generateHeaderFileName(String fileNameBase)
  {
    return String.format("%s_header", fileNameBase);
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
    headerOut = new OutputBytes();
    valuesOut = new OutputBytes();
  }

  public void write(T objectToWrite) throws IOException
  {
    if (objectsSorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      objectsSorted = false;
    }

    ++numWritten;
    valuesOut.writeInt(0);
    strategy.writeTo(objectToWrite, valuesOut);

    if (!requireMultipleFiles) {
      headerOut.writeInt(Ints.checkedCast(valuesOut.size()));
    } else {
      headerOutLong.add(valuesOut.size());
    }

    if (!requireMultipleFiles && getSerializedSize() > fileSizeLimit) {
      requireMultipleFiles = true;
      initializeHeaderOutLong();
    }

    if (objectsSorted) {
      prevObject = objectToWrite;
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    if (requireMultipleFiles) {
      return metaSize();
    } else {
      return metaSize() + headerOut.size() + valuesOut.size();
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    if (requireMultipleFiles) {
      closeMultiFiles(channel, smoosher);
    } else {
      writeToSingleFile(channel);
    }
  }

  private void writeToSingleFile(WritableByteChannel channel) throws IOException
  {
    final long numBytesWritten = headerOut.size() + valuesOut.size();

    Preconditions.checkState(
        headerOut.size() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.size()
    );
    Preconditions.checkState(
        numBytesWritten < fileSizeLimit, "Wrote[%s] bytes, which is too many.",
        numBytesWritten
    );

    ByteBuffer meta = ByteBuffer.allocate(metaSize());
    meta.put(GenericIndexed.VERSION_ONE);
    meta.put(objectsSorted ? GenericIndexed.REVERSE_LOOKUP_ALLOWED : GenericIndexed.REVERSE_LOOKUP_DISALLOWED);
    meta.putInt(Ints.checkedCast(numBytesWritten + Integer.BYTES));
    meta.putInt(numWritten);
    meta.flip();

    Channels.writeFully(channel, meta);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  private void closeMultiFiles(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
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
    ByteBuffer meta = ByteBuffer.allocate(metaSize());
    meta.put(GenericIndexed.VERSION_TWO);
    meta.put(objectsSorted ? GenericIndexed.REVERSE_LOOKUP_ALLOWED : GenericIndexed.REVERSE_LOOKUP_DISALLOWED);
    meta.putInt(bagSizePower);
    meta.putInt(numWritten);
    meta.putInt(fileNameByteArray.length);
    meta.put(fileNameByteArray);
    meta.flip();
    Channels.writeFully(channel, meta);

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
  private int bagSizePower() throws IOException
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
   * @param powerTwo   candidate value split expressed as power of 2.
   *
   * @return true if candidate value split can hold all splits.
   *
   * @throws IOException
   */
  private boolean actuallyFits(int powerTwo) throws IOException
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
        currentValueOffset = headerOutLong.getLong(Ints.checkedCast(headerIndex + bagSize - 1));
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

  private int metaSize()
  {
    // for version 2 getSerializedSize() returns number of bytes in meta file.
    if (!requireMultipleFiles) {
      return 2 + // version and sorted flag
             Ints.BYTES + // numBytesWritten
             Ints.BYTES; // numWritten
    } else {
      return 2 + // version and sorted flag
             Ints.BYTES + // numElements as log base 2.
             Ints.BYTES + // number of files
             Ints.BYTES + // column name Size
             fileNameByteArray.length;
    }
  }

  private void writeHeaderLong(FileSmoosher smoosher, int bagSizePower)
      throws IOException
  {
    ByteBuffer helperBuffer = ByteBuffer.allocate(Ints.BYTES).order(ByteOrder.nativeOrder());

    int numberOfElementsPerValueFile = 1 << bagSizePower;
    long currentNumBytes = 0;
    long relativeRefBytes = 0;
    long relativeNumBytes;
    try (SmooshedWriter smooshChannel = smoosher
        .addWithSmooshedWriter(generateHeaderFileName(filenameBase), numWritten * Integer.BYTES)) {

      // following block converts long header indexes into int header indexes.
      for (int pos = 0; pos < numWritten; pos++) {
        //conversion of header offset from long to int completed for one value file done, change relativeRefBytes
        // to current offset.
        if ((pos & (numberOfElementsPerValueFile - 1)) == 0) {
          relativeRefBytes = currentNumBytes;
        }
        currentNumBytes = headerOutLong.getLong(pos);
        relativeNumBytes = currentNumBytes - relativeRefBytes;
        helperBuffer.putInt(0, Ints.checkedCast(relativeNumBytes));
        helperBuffer.clear();
        smooshChannel.write(helperBuffer);
      }
    }
  }

  private void initializeHeaderOutLong() throws IOException
  {
    headerOutLong = new LongArrayList();
    DataInput headerOutAsIntInput = new DataInputStream(headerOut.asInputStream());
    for (int i = 0; i < numWritten; i++) {
      int count = headerOutAsIntInput.readInt();
      headerOutLong.add(count);
    }
  }

}
