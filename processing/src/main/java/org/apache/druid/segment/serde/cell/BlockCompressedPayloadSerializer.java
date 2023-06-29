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

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class BlockCompressedPayloadSerializer implements Serializer
{
  private final IntSerializer intSerializer = new IntSerializer();
  private final BlockIndexWriter blockIndexWriter;
  private final WriteOutBytes dataOutBytes;

  public BlockCompressedPayloadSerializer(BlockIndexWriter blockIndexWriter, WriteOutBytes dataOutBytes)
  {
    this.blockIndexWriter = blockIndexWriter;
    this.dataOutBytes = dataOutBytes;
  }

  @Override
  public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
  {
    blockIndexWriter.transferTo(channel);
    channel.write(intSerializer.serialize(dataOutBytes.size()));
    dataOutBytes.writeTo(channel);
  }

  @Override
  public long getSerializedSize()
  {
    return blockIndexWriter.getSerializedSize()
           + intSerializer.getSerializedSize()
           + Ints.checkedCast(dataOutBytes.size());
  }
}
