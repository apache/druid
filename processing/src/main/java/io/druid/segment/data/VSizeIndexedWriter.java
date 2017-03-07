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
import io.druid.java.util.common.io.smoosh.FileSmoosher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * Streams arrays of objects out in the binary format described by VSizeIndexed
 */
public class VSizeIndexedWriter extends MultiValueIndexedIntsWriter
{
  private static final byte VERSION = 0x1;
  private static final byte[] EMPTY_ARRAY = new byte[]{};

  private final int maxId;

  private OutputBytes headerOut = null;
  private OutputBytes valuesOut = null;
  private int numWritten = 0;
  private boolean numBytesForMaxWritten = false;

  public VSizeIndexedWriter(int maxId)
  {
    this.maxId = maxId;
  }

  @Override
  public void open() throws IOException
  {
    headerOut = new OutputBytes();
    valuesOut = new OutputBytes();
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

    headerOut.writeInt(Ints.checkedCast(valuesOut.size()));

    ++numWritten;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeNumBytesForMax();
    return metaSize() + headerOut.size() + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeNumBytesForMax();

    final long numBytesWritten = headerOut.size() + valuesOut.size();

    Preconditions.checkState(
        headerOut.size() == (numWritten * 4),
        "numWritten[%s] number of rows should have [%s] bytes written to headerOut, had[%s]",
        numWritten,
        numWritten * 4,
        headerOut.size()
    );
    Preconditions.checkState(
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );

    ByteBuffer meta = ByteBuffer.allocate(metaSize());
    meta.put(VERSION);
    meta.put(VSizeIndexedInts.getNumBytesForMax(maxId));
    meta.putInt((int) numBytesWritten + 4);
    meta.putInt(numWritten);
    meta.flip();

    Channels.writeFully(channel, meta);
    headerOut.writeTo(channel);
    valuesOut.writeTo(channel);
  }

  private void writeNumBytesForMax() throws IOException
  {
    if (!numBytesForMaxWritten) {
      final byte numBytesForMax = VSizeIndexedInts.getNumBytesForMax(maxId);
      valuesOut.write(new byte[4 - numBytesForMax]);
      numBytesForMaxWritten = true;
    }
  }

  private int metaSize()
  {
    return 1 +    // version
           1 +    // numBytes
           4 +    // numBytesWritten
           4;     // numElements
  }
}
