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

package org.apache.druid.segment.serde.cell;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.Serializer;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;


public class CellIndexWriter implements Serializer, Closeable
{
  private final LongSerializer longSerializer = new LongSerializer();
  private final BlockCompressedPayloadWriter payloadWriter;

  private long position = 0;
  private boolean open = true;

  public CellIndexWriter(BlockCompressedPayloadWriter payloadWriter)
  {
    this.payloadWriter = payloadWriter;
  }

  public void persistAndIncrement(int increment) throws IOException
  {
    Preconditions.checkArgument(increment >= 0);
    Preconditions.checkState(open, "cannot write to closed CellIndex");
    payloadWriter.write(longSerializer.serialize(position));
    position += increment;
  }

  @Override
  public void close() throws IOException
  {
    if (open) {
      payloadWriter.write(longSerializer.serialize(position));
      payloadWriter.close();
      open = false;
    }
  }

  @Override
  public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
  {
    Preconditions.checkState(!open, "cannot transfer a CellIndex that is not closed and finalized");

    payloadWriter.writeTo(channel, smoosher);
  }

  @Override
  public long getSerializedSize()
  {
    return payloadWriter.getSerializedSize();
  }
}
