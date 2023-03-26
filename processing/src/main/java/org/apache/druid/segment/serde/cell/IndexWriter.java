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
import com.google.common.primitives.Ints;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class IndexWriter
{
  private final WriteOutBytes outBytes;
  private final NumberSerializer positionSerializer;
  private final NumberSerializer indexSizeSerializer;

  private boolean open = true;
  private long position = 0;

  public IndexWriter(
      WriteOutBytes outBytes,
      NumberSerializer positionSerializer,
      NumberSerializer indexSizeSerializer
  )
  {
    this.outBytes = outBytes;
    this.positionSerializer = positionSerializer;
    this.indexSizeSerializer = indexSizeSerializer;
  }

  public IndexWriter(WriteOutBytes outBytes, NumberSerializer positionSerializer)
  {
    this(outBytes, positionSerializer, new IntSerializer());
  }

  public void persistAndIncrement(int increment) throws IOException
  {
    Preconditions.checkArgument(increment >= 0, "increment must be non-negative");
    Preconditions.checkState(open, "peristAndIncrement() must be called when open");
    outBytes.write(positionSerializer.serialize(position));
    position += increment;
  }

  public void close() throws IOException
  {
    // when done, write an n+1'th entry for the next unused block; this lets us the use invariant
    // of length of block i = entry i+1 - entry i for all i < n
    outBytes.write(positionSerializer.serialize(position));
    open = false;
  }

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    channel.write(indexSizeSerializer.serialize(outBytes.size()));
    outBytes.writeTo(channel);
  }

  public long getSerializedSize()
  {
    return indexSizeSerializer.getSerializedSize() + Ints.checkedCast(outBytes.size());
  }
}
