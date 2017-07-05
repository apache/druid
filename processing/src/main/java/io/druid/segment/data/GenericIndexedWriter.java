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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;


/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements Closeable
{
  private static int PAGE_SIZE = 4096;
  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;
  private final int fileSizeLimit;
  private final byte[] fileNameByteArray;
  private boolean objectsSorted = true;
  private T prevObject = null;
  private CountingOutputStream headerOut = null;
  private CountingOutputStream valuesOut = null;
  private CountingOutputStream headerOutLong = null;
  private long numWritten = 0;
  private boolean requireMultipleFiles = false;
  private ByteBuffer buf;
  private final ByteBuffer sizeHelperBuffer = ByteBuffer.allocate(Ints.BYTES);


  public GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this(ioPeon, filenameBase, strategy, Integer.MAX_VALUE & ~PAGE_SIZE);
  }

  public GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy,
      int fileSizeLimit
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.fileSizeLimit = fileSizeLimit;
    fileNameByteArray = StringUtils.toUtf8(filenameBase);
    buf = ByteBuffer.allocate(Ints.BYTES);
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
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("values")));
  }

  public void write(T objectToWrite) throws IOException
  {
    if (objectsSorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      objectsSorted = false;
    }

    byte[] bytesToWrite = strategy.toBytes(objectToWrite);

    ++numWritten;
    SerializerUtils.writeBigEndianIntToOutputStream(valuesOut, bytesToWrite.length, sizeHelperBuffer);
    valuesOut.write(bytesToWrite);

    if (!requireMultipleFiles) {
      SerializerUtils.writeBigEndianIntToOutputStream(headerOut, Ints.checkedCast(valuesOut.getCount()), buf);
    } else {
      SerializerUtils.writeNativeOrderedLongToOutputStream(headerOutLong, valuesOut.getCount(), buf);
    }

    if (!requireMultipleFiles && getSerializedSize() > fileSizeLimit) {
      requireMultipleFiles = true;
      initializeHeaderOutLong();
      buf = ByteBuffer.allocate(Longs.BYTES).order(ByteOrder.nativeOrder());
    }

    prevObject = objectToWrite;
  }

  private String makeFilename(String suffix)
  {
    return StringUtils.format("%s.%s", filenameBase, suffix);
  }

  @Override
  public void close() throws IOException
  {
    valuesOut.close();
    if (requireMultipleFiles) {
      closeMultiFiles();
    } else {
      closeSingleFile();
    }
  }

  private void closeSingleFile() throws IOException
  {
    headerOut.close();
    final long numBytesWritten = headerOut.getCount() + valuesOut.getCount();

    Preconditions.checkState(
        headerOut.getCount() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.getCount()
    );
    Preconditions.checkState(
        numBytesWritten < fileSizeLimit, "Wrote[%s] bytes to base file %s, which is too many.",
        numBytesWritten,
        filenameBase
    );

    try (OutputStream metaOut = ioPeon.makeOutputStream(makeFilename("meta"))) {
      metaOut.write(GenericIndexed.VERSION_ONE);
      metaOut.write(objectsSorted ? GenericIndexed.REVERSE_LOOKUP_ALLOWED : GenericIndexed.REVERSE_LOOKUP_DISALLOWED);
      metaOut.write(Ints.toByteArray(Ints.checkedCast(numBytesWritten + 4)));
      metaOut.write(Ints.toByteArray(Ints.checkedCast(numWritten)));
    }
  }

  private void closeMultiFiles() throws IOException
  {
    headerOutLong.close();
    Preconditions.checkState(
        headerOutLong.getCount() == (numWritten * Longs.BYTES),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOutLong, had[%s]",
        numWritten,
        numWritten * Longs.BYTES,
        headerOutLong.getCount()
    );
    Preconditions.checkState(
        headerOutLong.getCount() < (Integer.MAX_VALUE & ~PAGE_SIZE),
        "Wrote[%s] bytes in header file of base file %s, which is too many.",
        headerOutLong.getCount(),
        filenameBase
    );
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
    long avgObjectSize = (valuesOut.getCount() + numWritten - 1) / numWritten;

    File f = ioPeon.getFile(makeFilename("headerLong"));
    Preconditions.checkNotNull(f, "header file missing.");

    try (RandomAccessFile headerFile = new RandomAccessFile(f, "r")) {
      for (int i = 31; i >= 0; --i) {
        if ((1L << i) * avgObjectSize <= fileSizeLimit) {
          if (actuallyFits(i, headerFile)) {
            return i;
          }
        }
      }
    }
    throw new ISE(
        "no value split found with fileSizeLimit [%d], avgObjectSize [%d] while serializing [%s]",
        fileSizeLimit,
        avgObjectSize,
        filenameBase
    );
  }

  /**
   * Checks if candidate value splits can divide value file in such a way no object/element crosses the value splits.
   *
   * @param powerTwo   candidate value split expressed as power of 2.
   * @param headerFile header file.
   *
   * @return true if candidate value split can hold all splits.
   *
   * @throws IOException
   */
  private boolean actuallyFits(int powerTwo, RandomAccessFile headerFile) throws IOException
  {
    long lastValueOffset = 0;
    long currentValueOffset = 0;
    long valueBytesWritten = valuesOut.getCount();
    long headerIndex = 0;
    long bagSize = 1L << powerTwo;

    while (lastValueOffset < valueBytesWritten) {

      if (headerIndex >= numWritten) {
        return true;
      } else if (headerIndex + bagSize <= numWritten) {
        headerFile.seek((headerIndex + bagSize - 1) * Longs.BYTES);
        currentValueOffset = Long.reverseBytes(headerFile.readLong());
      } else if (numWritten < headerIndex + bagSize) {
        headerFile.seek((numWritten - 1) * Longs.BYTES);
        currentValueOffset = Long.reverseBytes(headerFile.readLong());
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

  public long getSerializedSize()
  {
    // for version 2 getSerializedSize() returns number of bytes in meta file.
    if (!requireMultipleFiles) {
      return 2 + // version and sorted flag
             Ints.BYTES + // numBytesWritten
             Ints.BYTES + // numElements
             headerOut.getCount() + // header length
             valuesOut.getCount(); // value length
    } else {
      return 2 + // version and sorted flag
             Ints.BYTES + // numElements as log base 2.
             Ints.BYTES + // number of files
             Ints.BYTES + // column name Size
             fileNameByteArray.length;
    }
  }

  @Deprecated
  public InputSupplier<InputStream> combineStreams()
  {
    // ByteSource.concat is only available in guava 15 and higher
    // This is guava 14 compatible
    if (requireMultipleFiles) {
      throw new ISE("Can not combine streams for version 2."); //fallback to old behaviour.
    }

    return ByteStreams.join(
        Iterables.transform(
            Arrays.asList("meta", "header", "values"),
            new Function<String, InputSupplier<InputStream>>()
            {
              @Override
              public InputSupplier<InputStream> apply(final String input)
              {
                return new InputSupplier<InputStream>()
                {
                  @Override
                  public InputStream getInput() throws IOException
                  {
                    return ioPeon.makeInputStream(makeFilename(input));
                  }
                };
              }
            }
        )
    );
  }

  private void writeToChannelVersionOne(WritableByteChannel channel) throws IOException
  {
    try (ReadableByteChannel from = Channels.newChannel(combineStreams().getInput())) {
      ByteStreams.copy(from, channel);
    }

  }

  private void writeToChannelVersionTwo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    if (smoosher == null) {
      throw new IAE("version 2 GenericIndexedWriter requires FileSmoosher.");
    }

    int bagSizePower = bagSizePower();
    OutputStream metaOut = Channels.newOutputStream(channel);
    metaOut.write(GenericIndexed.VERSION_TWO);
    metaOut.write(objectsSorted ? GenericIndexed.REVERSE_LOOKUP_ALLOWED : GenericIndexed.REVERSE_LOOKUP_DISALLOWED);
    metaOut.write(Ints.toByteArray(bagSizePower));
    metaOut.write(Ints.toByteArray(Ints.checkedCast(numWritten)));
    metaOut.write(Ints.toByteArray(fileNameByteArray.length));
    metaOut.write(fileNameByteArray);

    try (RandomAccessFile headerFile = new RandomAccessFile(ioPeon.getFile(makeFilename("headerLong")), "r")) {
      Preconditions.checkNotNull(headerFile, "header file missing.");
      long previousValuePosition = 0;
      int bagSize = 1 << bagSizePower;

      int numberOfFilesRequired = GenericIndexed.getNumberOfFilesRequired(bagSize, numWritten);
      byte[] buffer = new byte[1 << 16];

      try (InputStream is = new FileInputStream(ioPeon.getFile(makeFilename("values")))) {
        int counter = -1;

        for (int i = 0; i < numberOfFilesRequired; i++) {
          if (i != numberOfFilesRequired - 1) {
            headerFile.seek((bagSize + counter) * Longs.BYTES); // 8 for long bytes.
            counter = counter + bagSize;
          } else {
            headerFile.seek((numWritten - 1) * Longs.BYTES); // for remaining items.
          }

          long valuePosition = Long.reverseBytes(headerFile.readLong());
          long numBytesToPutInFile = valuePosition - previousValuePosition;

          try (SmooshedWriter smooshChannel = smoosher
              .addWithSmooshedWriter(generateValueFileName(filenameBase, i), numBytesToPutInFile)) {
            writeBytesIntoSmooshedChannel(numBytesToPutInFile, buffer, smooshChannel, is);
            previousValuePosition = valuePosition;
          }
        }
      }
      writeHeaderLong(smoosher, headerFile, bagSizePower, buffer);
    }
  }

  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    if (!requireMultipleFiles) {
      writeToChannelVersionOne(channel);
    } else {
      writeToChannelVersionTwo(channel, smoosher);
    }
  }

  private void writeHeaderLong(FileSmoosher smoosher, RandomAccessFile headerFile, int bagSizePower, byte[] buffer)
      throws IOException
  {
    ByteBuffer helperBuffer = ByteBuffer.allocate(Ints.BYTES).order(ByteOrder.nativeOrder());

    try (CountingOutputStream finalHeaderOut = new CountingOutputStream(
        ioPeon.makeOutputStream(makeFilename("header_final")))) {
      int numberOfElementsPerValueFile = 1 << bagSizePower;
      long currentNumBytes = 0;
      long relativeRefBytes = 0;
      long relativeNumBytes;
      headerFile.seek(0);

      // following block converts long header indexes into int header indexes.
      for (int pos = 0; pos < numWritten; pos++) {
        //conversion of header offset from long to int completed for one value file done, change relativeRefBytes
        // to current offset.
        if ((pos & (numberOfElementsPerValueFile - 1)) == 0) {
          relativeRefBytes = currentNumBytes;
        }
        currentNumBytes = Long.reverseBytes(headerFile.readLong());
        relativeNumBytes = currentNumBytes - relativeRefBytes;
        SerializerUtils.writeNativeOrderedIntToOutputStream(
            finalHeaderOut,
            Ints.checkedCast(relativeNumBytes),
            helperBuffer
        );
      }

      long numBytesToPutInFile = finalHeaderOut.getCount();
      finalHeaderOut.close();
      try (InputStream is = new FileInputStream(ioPeon.getFile(makeFilename("header_final")))) {
        try (SmooshedWriter smooshChannel = smoosher
            .addWithSmooshedWriter(generateHeaderFileName(filenameBase), numBytesToPutInFile)) {
          writeBytesIntoSmooshedChannel(numBytesToPutInFile, buffer, smooshChannel, is);
        }
      }

    }
  }

  private void initializeHeaderOutLong() throws IOException
  {
    headerOut.close();
    headerOutLong = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("headerLong")));

    try (RandomAccessFile headerFile = new RandomAccessFile(ioPeon.getFile(makeFilename("header")), "r")) {
      ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES).order(ByteOrder.nativeOrder());
      for (int i = 0; i < numWritten; i++) {
        int count = headerFile.readInt();
        SerializerUtils.writeNativeOrderedLongToOutputStream(headerOutLong, count, buf);
      }
    }
  }

}
