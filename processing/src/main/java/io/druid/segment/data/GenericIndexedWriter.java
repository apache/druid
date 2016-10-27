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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.java.util.common.io.smoosh.SmooshedWriter;


/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements Closeable
{
  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ObjectStrategy<T> strategy;

  private boolean objectsSorted = true;
  private T prevObject = null;

  private CountingOutputStream headerOut = null;
  private CountingOutputStream valuesOut = null;
  private CountingOutputStream headerOutLong = null;
  private long numWritten = 0;
  private int bagSizePower=0;
  private boolean useVersionThree = false;
  private final int fileSizeLimit;
  private final byte [] fileNameByteArray ;

  public GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this(ioPeon, filenameBase, strategy, Integer.MAX_VALUE);
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
    fileNameByteArray = filenameBase.getBytes();
  }

  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("header")));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("values")));
    headerOutLong = new CountingOutputStream(ioPeon.makeOutputStream(makeFilename("headerLong")));
  }

  public void write(T objectToWrite) throws IOException
  {
    if (objectsSorted && prevObject != null && strategy.compare(prevObject, objectToWrite) >= 0) {
      objectsSorted = false;
    }

    byte[] bytesToWrite = strategy.toBytes(objectToWrite);

    ++numWritten;
    valuesOut.write(Ints.toByteArray(bytesToWrite.length));
    valuesOut.write(bytesToWrite);

    headerOut.write(Ints.toByteArray((int) valuesOut.getCount()));
    headerOutLong.write(Longs.toByteArray(valuesOut.getCount()));

    if (getSerializedSize() > fileSizeLimit) {
      useVersionThree = true;
    }

    prevObject = objectToWrite;
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  @Override
  public void close() throws IOException
  {
    if (useVersionThree) {
      closeMultiFiles();
    } else {
      closeSingleFile();
    }
  }

  private void closeSingleFile() throws IOException
  {
    headerOut.close();
    headerOutLong.close();
    valuesOut.close();

    final long numBytesWritten = headerOut.getCount() + valuesOut.getCount();

    Preconditions.checkState(
        headerOut.getCount() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.getCount()
    );
    Preconditions.checkState(
        numBytesWritten < fileSizeLimit, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );

    try (OutputStream metaOut = ioPeon.makeOutputStream(makeFilename("meta"))) {
      metaOut.write(0x1);
      metaOut.write(objectsSorted ? 0x1 : 0x0);
      metaOut.write(Ints.toByteArray((int) numBytesWritten + 4));
      metaOut.write(Ints.toByteArray((int) numWritten));
    }
  }

  private void closeMultiFiles() throws IOException
  {
    headerOut.close();
    headerOutLong.close();
    valuesOut.close();

    final long numBytesWritten = headerOutLong.getCount() + valuesOut.getCount();

    // revisit this check.
    Preconditions.checkState(
        headerOutLong.getCount() == (numWritten * 8),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 8,
        headerOutLong.getCount());
    Preconditions.checkState(
        headerOutLong.getCount() < Integer.MAX_VALUE,
        "Wrote[%s] bytes in header file, which is too many.",
        headerOutLong.getCount());

    bagSizePower = bagSizePower();
    try (OutputStream metaOut = ioPeon.makeOutputStream(makeFilename("meta"))){
      metaOut.write(0x3);// new meta version.
      metaOut.write(objectsSorted ? 0x1 : 0x0);
      metaOut.write(Ints.toByteArray(bagSizePower));
      metaOut.write(Longs.toByteArray(numBytesWritten + Ints.BYTES + Ints.BYTES + fileNameByteArray.length));//number of bytes
      metaOut.write(Ints.toByteArray((int) numWritten));
      metaOut.write(Ints.toByteArray(fileNameByteArray.length));
      metaOut.write(fileNameByteArray);
    }
  }

  /**
   * Tries to get best value split(number of elements in each value file) which can be expressed as power of 2.
   * @return Returns the size of value file splits as power of 2.
   * @throws IOException
   */
  private int bagSizePower() throws IOException
  {
    int avgObjectSize = (int) (valuesOut.getCount() / numWritten);

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
    throw new IllegalStateException("no value split found.");
  }

  /**
   * Checks if candidate value splits can divide value file in such a way no object/element crosses the value splits.
   *
   * @param powerTwo candidate value split expressed as power of 2.
   * @param headerFile header file.
   * @return true if candidate value split can hold all splits.
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

      if (headerIndex + bagSize <= numWritten) {
        headerFile.seek((headerIndex + bagSize - 1) * 8);
        currentValueOffset = headerFile.readLong();
      } else if (headerIndex < numWritten && numWritten < (headerIndex + bagSize)) {
        headerFile.seek((numWritten - 1) * 8);
        currentValueOffset = headerFile.readLong();
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
    // for version 3 getSerializedSize() returns number of bytes in meta file.
    return !useVersionThree ? 2 + // version and sorted flag
           Ints.BYTES + // numBytesWritten
           Ints.BYTES + // numElements
           headerOut.getCount() + // header length
           valuesOut.getCount() // value length
           : 2 + // version and sorted flag
           Ints.BYTES + // numBytesWritten
           Longs.BYTES + // numElements
           Ints.BYTES + // number of files
           Ints.BYTES + // column name Size
           fileNameByteArray.length;
  }

  @Deprecated
  public InputSupplier<InputStream> combineStreams()
  {
    // ByteSource.concat is only available in guava 15 and higher
    // This is guava 14 compatible
    if(useVersionThree)
    {
      throw new ISE("Can not combine streams for version 3."); //fallback to old behaviour.
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

  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    ReadableByteChannel from;
    if(!useVersionThree) {
      from = Channels.newChannel(combineStreams().getInput());
      ByteStreams.copy(from, channel);
      return;
    }
    else {
      from = Channels.newChannel
          ( ByteStreams.join
              (
                  Iterables.transform(
                      Arrays.asList("meta"),
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
              ).getInput()
          );
      ByteStreams.copy(from, channel);
    }

    if (smoosher == null) {
      throw new IAE("version 3 requires FileSmoosher.");
    }
    File f = ioPeon.getFile(makeFilename("headerLong"));
    Preconditions.checkNotNull(f, "header file missing.");

    try (RandomAccessFile headerFile = new RandomAccessFile(f, "r"))
    {
      long previousValueOffsetInValueFile = 0;
      int bagSize = 1 << bagSizePower;
      int filesRequired = (int) (numWritten / bagSize);

      try (InputStream is = new FileInputStream(ioPeon.getFile(makeFilename("values"))))
      {
        int counter = -1;
        for (long i = 0; i <= filesRequired; i++) {
          if (i != (filesRequired)) {
            headerFile.seek((bagSize + counter) * 8); // 8 for long bytes.
            counter = counter + bagSize;
          } else {
            headerFile.seek((numWritten - 1) * 8); // for remaining items.
          }
          long valuePosition = headerFile.readLong();
          byte[] buffer = new byte[1 << 16];
          long numBytesToPutInFile = valuePosition - previousValueOffsetInValueFile;

          try (SmooshedWriter smooshChannel = smoosher
              .addWithSmooshedWriter(String.format("%s_value_%d", filenameBase, i), numBytesToPutInFile))
          {

            while (numBytesToPutInFile > 0) {
              int bytesRead = is.read(buffer, 0, Math.min(buffer.length, (int) numBytesToPutInFile));
              smooshChannel.write((ByteBuffer) ByteBuffer.wrap(buffer).limit(bytesRead));
              numBytesToPutInFile -= bytesRead;
            }
            previousValueOffsetInValueFile = valuePosition;
          }
        }
      }
      writeHeader(smoosher, headerFile);
    }
  }

  private void writeHeader(FileSmoosher smoosher, RandomAccessFile headerFile) throws IOException
  {
    try (CountingOutputStream finalHeaderOut = new CountingOutputStream(
        ioPeon.makeOutputStream(makeFilename("header_final")))) {
      int bagSize = 1 << bagSizePower;
      long currentNumBytes = 0, relativeRefBytes = 0, relativeNumBytes;

      for (int pos = 0; pos < numWritten; pos++)
      {
        int relativePosition = pos % bagSize;
        if (relativePosition == 0) {
          relativeRefBytes = currentNumBytes;
        }
        headerFile.seek(pos * Longs.BYTES);
        currentNumBytes = headerFile.readLong();
        relativeNumBytes = currentNumBytes - relativeRefBytes;
        finalHeaderOut.write(Ints.toByteArray((int) (relativeNumBytes)));
      }

      long numBytesToPutInFile = finalHeaderOut.getCount();
      finalHeaderOut.close();
      byte[] buffer = new byte[1 << 16];
      try (InputStream is = new FileInputStream(ioPeon.getFile(makeFilename("header_final"))))
      {
        try (SmooshedWriter smooshChannel = smoosher
            .addWithSmooshedWriter(String.format("%s_header", filenameBase), numBytesToPutInFile))
        {
          while (numBytesToPutInFile > 0) {
            int bytesRead = is.read(buffer, 0, Math.min(buffer.length, (int) numBytesToPutInFile));
            smooshChannel.write((ByteBuffer) ByteBuffer.wrap(buffer).limit(bytesRead));
            numBytesToPutInFile -= bytesRead;
          }
        }
      }

    }
  }

}
