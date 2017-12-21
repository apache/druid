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
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.writeout.WriteOutBytes;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.serde.MetaSerdeHelper;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 * Streams arrays of objects out in the binary format described by VSizeIndexed
 */
public class VSizeIndexedWriter extends MultiValueIndexedIntsWriter
{
  private static final byte VERSION = 0x1;

  private static final MetaSerdeHelper<VSizeIndexedWriter> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((VSizeIndexedWriter x) -> VERSION)
      .writeByte(x -> VSizeIndexedInts.getNumBytesForMax(x.maxId))
      .writeInt(x -> Ints.checkedCast(x.headerOut.size() + x.valuesOut.size() + Integer.BYTES))
      .writeInt(x -> x.numWritten);

  private enum WriteInt
  {
    ONE_BYTE {
      @Override
      void write(WriteOutBytes out, int v) throws IOException
      {
        out.write(v);
      }
    },
    TWO_BYTES {
      @Override
      void write(WriteOutBytes out, int v) throws IOException
      {
        out.write(v >> 8);
        out.write(v);
      }
    },
    THREE_BYTES {
      @Override
      void write(WriteOutBytes out, int v) throws IOException
      {
        out.write(v >> 16);
        out.write(v >> 8);
        out.write(v);
      }
    },
    FOUR_BYTES {
      @Override
      void write(WriteOutBytes out, int v) throws IOException
      {
        out.writeInt(v);
      }
    };

    abstract void write(WriteOutBytes out, int v) throws IOException;
  }

  private final int maxId;
  private final WriteInt writeInt;

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private WriteOutBytes headerOut = null;
  private WriteOutBytes valuesOut = null;
  private int numWritten = 0;
  private boolean numBytesForMaxWritten = false;

  public VSizeIndexedWriter(SegmentWriteOutMedium segmentWriteOutMedium, int maxId)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.maxId = maxId;
    this.writeInt = WriteInt.values()[VSizeIndexedInts.getNumBytesForMax(maxId) - 1];
  }

  @Override
  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  protected void addValues(IntList ints) throws IOException
  {
    if (numBytesForMaxWritten) {
      throw new IllegalStateException("written out already");
    }
    if (ints != null) {
      for (int i = 0; i < ints.size(); i++) {
        int value = ints.getInt(i);
        Preconditions.checkState(value >= 0 && value <= maxId);
        writeInt.write(valuesOut, value);
      }
    }
    headerOut.writeInt(Ints.checkedCast(valuesOut.size()));
    ++numWritten;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeNumBytesForMax();
    return metaSerdeHelper.size(this) + headerOut.size() + valuesOut.size();
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
        numBytesWritten < Integer.MAX_VALUE - Integer.BYTES,
        "Wrote[%s] bytes, which is too many.",
        numBytesWritten
    );

    metaSerdeHelper.writeTo(channel, this);
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
}
