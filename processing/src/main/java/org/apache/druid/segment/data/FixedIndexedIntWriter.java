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

import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.io.Channels;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Specialized version of {@link FixedIndexedWriter} for writing ints, with no support for null values, and no
 * verification that data is actually sorted, it just trusts you and takes your word for it
 */
public final class FixedIndexedIntWriter implements Serializer
{
  private static final int PAGE_SIZE = 4096;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final ByteBuffer scratch;
  private int numWritten;
  @Nullable
  private WriteOutBytes valuesOut = null;

  private final boolean isSorted;

  public FixedIndexedIntWriter(SegmentWriteOutMedium segmentWriteOutMedium, boolean sorted)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    // this is a matter of faith, nothing checks
    this.isSorted = sorted;
    this.scratch = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());
  }

  public void open() throws IOException
  {
    this.valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public long getSerializedSize()
  {
    return Byte.BYTES + Byte.BYTES + Integer.BYTES + valuesOut.size();
  }

  public void write(int objectToWrite) throws IOException
  {
    scratch.clear();
    scratch.putInt(objectToWrite);
    scratch.flip();
    Channels.writeFully(valuesOut, scratch);
    numWritten++;
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    scratch.clear();
    // version 0
    scratch.put((byte) 0);
    // no flags, this thing is never sorted
    byte flags = 0x00;
    if (isSorted) {
      flags = (byte) (flags | FixedIndexed.IS_SORTED_MASK);
    }
    scratch.put(flags);
    scratch.flip();
    Channels.writeFully(channel, scratch);
    scratch.clear();
    scratch.putInt(numWritten);
    scratch.flip();
    Channels.writeFully(channel, scratch);
    valuesOut.writeTo(channel);
  }

  public IntIterator getIterator()
  {
    final ByteBuffer iteratorBuffer = ByteBuffer.allocate(Integer.BYTES * PAGE_SIZE).order(ByteOrder.nativeOrder());

    return new IntIterator()
    {
      @Override
      public int nextInt()
      {
        if (pos == 0 || iteratorBuffer.position() >= iteratorBuffer.limit()) {
          readPage();
        }
        final int value = iteratorBuffer.getInt();
        pos++;
        return value;
      }

      int pos = 0;
      @Override
      public boolean hasNext()
      {
        return pos < numWritten;
      }

      private void readPage()
      {
        iteratorBuffer.clear();
        try {
          if (numWritten - pos < PAGE_SIZE) {
            int size = (numWritten - pos) * Integer.BYTES;
            iteratorBuffer.limit(size);
            valuesOut.readFully((long) pos * Integer.BYTES, iteratorBuffer);
          } else {
            valuesOut.readFully((long) pos * Integer.BYTES, iteratorBuffer);
          }
          iteratorBuffer.flip();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
