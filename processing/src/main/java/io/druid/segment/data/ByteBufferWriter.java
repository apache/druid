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
import io.druid.output.OutputBytes;
import io.druid.output.OutputMedium;
import io.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

/**
 */
public class ByteBufferWriter<T> implements Serializer
{
  private final OutputMedium outputMedium;
  private final ObjectStrategy<T> strategy;

  private OutputBytes headerOut = null;
  private OutputBytes valueOut = null;

  public ByteBufferWriter(OutputMedium outputMedium, ObjectStrategy<T> strategy)
  {
    this.outputMedium = outputMedium;
    this.strategy = strategy;
  }

  public void open() throws IOException
  {
    headerOut = outputMedium.makeOutputBytes();
    valueOut = outputMedium.makeOutputBytes();
  }

  public void write(T objectToWrite) throws IOException
  {
    long sizeBefore = valueOut.size();
    strategy.writeTo(objectToWrite, valueOut);
    headerOut.writeInt(Ints.checkedCast(valueOut.size() - sizeBefore));
  }

  @Override
  public long getSerializedSize() throws IOException
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
