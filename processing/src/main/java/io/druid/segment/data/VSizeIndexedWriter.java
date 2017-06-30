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
import com.google.common.io.Closeables;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;

/**
 * Streams arrays of objects out in the binary format described by VSizeIndexed
 */
public class VSizeIndexedWriter extends MultiValueIndexedIntsWriter implements Closeable
{
  private static final byte VERSION = 0x1;
  private static final byte[] EMPTY_ARRAY = new byte[]{};

  private final int maxId;

  private CountingOutputStream headerOut = null;
  private CountingOutputStream valuesOut = null;
  int numWritten = 0;
  private final IOPeon ioPeon;
  private final String metaFileName;
  private final String headerFileName;
  private final String valuesFileName;

  public VSizeIndexedWriter(
      IOPeon ioPeon,
      String filenameBase,
      int maxId
  )
  {
    this.ioPeon = ioPeon;
    this.metaFileName = StringUtils.format("%s.meta", filenameBase);
    this.headerFileName = StringUtils.format("%s.header", filenameBase);
    this.valuesFileName = StringUtils.format("%s.values", filenameBase);
    this.maxId = maxId;
  }

  @Override
  public void open() throws IOException
  {
    headerOut = new CountingOutputStream(ioPeon.makeOutputStream(headerFileName));
    valuesOut = new CountingOutputStream(ioPeon.makeOutputStream(valuesFileName));
  }

  @Override
  protected void addValues(List<Integer> val) throws IOException
  {
    write(val);
  }

  public void write(List<Integer> ints) throws IOException
  {
    byte[] bytesToWrite = ints == null ? EMPTY_ARRAY : VSizeIndexedInts.getBytesNoPaddingFromList(ints, maxId);

    valuesOut.write(bytesToWrite);

    headerOut.write(Ints.toByteArray((int) valuesOut.getCount()));

    ++numWritten;
  }

  @Override
  public void close() throws IOException
  {
    final byte numBytesForMax = VSizeIndexedInts.getNumBytesForMax(maxId);

    valuesOut.write(new byte[4 - numBytesForMax]);

    Closeables.close(headerOut, false);
    Closeables.close(valuesOut, false);

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

    try (OutputStream metaOut = ioPeon.makeOutputStream(metaFileName)) {
      metaOut.write(new byte[]{VERSION, numBytesForMax});
      metaOut.write(Ints.toByteArray((int) numBytesWritten + 4));
      metaOut.write(Ints.toByteArray(numWritten));
    }
  }

  public InputSupplier<InputStream> combineStreams()
  {
    return ByteStreams.join(
        Iterables.transform(
            Arrays.asList(metaFileName, headerFileName, valuesFileName),
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
                    return ioPeon.makeInputStream(input);
                  }
                };
              }
            }
        )
    );
  }

  @Override
  public long getSerializedSize()
  {
    return 1 +    // version
           1 +    // numBytes
           4 +    // numBytesWritten
           4 +    // numElements
           headerOut.getCount() +
           valuesOut.getCount();
  }

  @Override
  public void writeToChannel(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    try (final ReadableByteChannel from = Channels.newChannel(combineStreams().getInput())) {
      ByteStreams.copy(from, channel);
    }
  }
}
