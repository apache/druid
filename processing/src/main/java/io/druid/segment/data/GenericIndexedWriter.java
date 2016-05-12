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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;

/**
 * Streams arrays of objects out in the binary format described by GenericIndexed
 */
public class GenericIndexedWriter<T> implements Closeable
{
  private final IOPeon ioPeon;
  private final String filenameBase;
  final ObjectStrategy<T> strategy;

  private final int version;
  boolean objectsSorted = true;
  T prevObject = null;

  CountingOutputStream headerOut = null;
  CountingOutputStream valuesOut = null;
  int numWritten = 0;

  private GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy
  )
  {
    this(ioPeon, filenameBase, strategy, 0x1);
  }

  GenericIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      ObjectStrategy<T> strategy,
      int version
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.strategy = strategy;
    this.version = version;
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
    valuesOut.write(Ints.toByteArray(bytesToWrite.length));
    valuesOut.write(bytesToWrite);

    headerOut.write(Ints.toByteArray((int) valuesOut.getCount()));

    prevObject = objectToWrite;
  }

  private String makeFilename(String suffix)
  {
    return String.format("%s.%s", filenameBase, suffix);
  }

  @Override
  public void close() throws IOException
  {
    headerOut.close();
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
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );

    OutputStream metaOut = ioPeon.makeOutputStream(makeFilename("meta"));

    try {
      metaOut.write(version);
      metaOut.write(objectsSorted ? 0x1 : 0x0);
      metaOut.write(Ints.toByteArray((int) numBytesWritten + 4));
      metaOut.write(Ints.toByteArray(numWritten));
    }
    finally {
      metaOut.close();
    }
  }

  public long getSerializedSize()
  {
    return 2 +                    // version and sorted flag
           Ints.BYTES +           // numBytesWritten
           Ints.BYTES +           // numElements
           headerOut.getCount() + // header length
           valuesOut.getCount();  // value length
  }

  public InputSupplier<InputStream> combineStreams()
  {
    return ByteStreams.join(
        Iterables.transform(
            Arrays.asList("meta", "header", "values"),
            new Function<String,InputSupplier<InputStream>>() {

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

  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    final ReadableByteChannel from = Channels.newChannel(combineStreams().getInput());
    ByteStreams.copy(from, channel);
  }
}
