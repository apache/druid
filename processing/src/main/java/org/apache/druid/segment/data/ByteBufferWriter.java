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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ByteBufferWriter<T> implements Serializer
{
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final ObjectStrategy<T> strategy;

  @Nullable
  private WriteOutBytes headerOut = null;
  @Nullable
  private WriteOutBytes valueOut = null;

  public ByteBufferWriter(SegmentWriteOutMedium segmentWriteOutMedium, ObjectStrategy<T> strategy)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.strategy = strategy;
  }

  public void open() throws IOException
  {
    headerOut = segmentWriteOutMedium.makeWriteOutBytes();
    valueOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  public void write(T objectToWrite) throws IOException
  {
    long sizeBefore = valueOut.size();
    strategy.writeTo(objectToWrite, valueOut);
    headerOut.writeInt(Ints.checkedCast(valueOut.size() - sizeBefore));
  }

  @Override
  public long getSerializedSize()
  {
    return headerOut.size() + valueOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    final long numBytesWritten = headerOut.size() + valueOut.size();
    Preconditions.checkState(
        numBytesWritten < Integer.MAX_VALUE, "Wrote[%s] bytes, which is too many.", numBytesWritten
    );

    headerOut.writeTo(channel);
    valueOut.writeTo(channel);
  }
}
