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

import com.google.common.primitives.Ints;
import org.apache.druid.common.utils.ByteUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Streams integers out in the binary format described by {@link VSizeColumnarInts}
 */
public class VSizeColumnarIntsSerializer extends SingleValueColumnarIntsSerializer
{
  private static final byte VERSION = VSizeColumnarInts.VERSION;

  private static final MetaSerdeHelper<VSizeColumnarIntsSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((VSizeColumnarIntsSerializer x) -> VERSION)
      .writeByte(x -> ByteUtils.checkedCast(x.numBytes))
      .writeInt(x -> Ints.checkedCast(x.valuesOut.size()));

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final int numBytes;

  private final ByteBuffer helperBuffer = ByteBuffer.allocate(Integer.BYTES);
  private boolean bufPaddingWritten = false;

  @Nullable
  private WriteOutBytes valuesOut = null;

  public VSizeColumnarIntsSerializer(final SegmentWriteOutMedium segmentWriteOutMedium, final int maxValue)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.numBytes = VSizeColumnarInts.getNumBytesForMax(maxValue);
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void addValue(int val) throws IOException
  {
    if (bufPaddingWritten) {
      throw new IllegalStateException("written out already");
    }
    helperBuffer.putInt(0, val);
    valuesOut.write(helperBuffer.array(), Integer.BYTES - numBytes, numBytes);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writeBufPadding();
    return META_SERDE_HELPER.size(this) + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writeBufPadding();
    META_SERDE_HELPER.writeTo(channel, this);
    valuesOut.writeTo(channel);
  }

  private void writeBufPadding() throws IOException
  {
    if (!bufPaddingWritten) {
      byte[] bufPadding = new byte[Integer.BYTES - numBytes];
      valuesOut.write(bufPadding);
      bufPaddingWritten = true;
    }
  }
}
