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

import org.apache.druid.java.util.common.IAE;

import java.nio.ByteBuffer;

public class DeltaLongEncodingReader implements CompressionFactory.LongEncodingReader
{

  private ByteBuffer buffer;
  private final long base;
  private final int bitsPerValue;
  private VSizeLongSerde.LongDeserializer deserializer;

  public DeltaLongEncodingReader(ByteBuffer fromBuffer)
  {
    this.buffer = fromBuffer.asReadOnlyBuffer();
    byte version = buffer.get();
    if (version == CompressionFactory.DELTA_ENCODING_VERSION) {
      base = buffer.getLong();
      bitsPerValue = buffer.getInt();
      fromBuffer.position(buffer.position());
      deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
    } else {
      throw new IAE("Unknown version[%s]", version);
    }
  }

  private DeltaLongEncodingReader(ByteBuffer buffer, long base, int bitsPerValue)
  {
    this.buffer = buffer;
    this.base = base;
    this.bitsPerValue = bitsPerValue;
    deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
  }

  @Override
  public void setBuffer(ByteBuffer buffer)
  {
    deserializer = VSizeLongSerde.getDeserializer(bitsPerValue, buffer, buffer.position());
  }

  @Override
  public long read(int index)
  {
    return base + deserializer.get(index);
  }

  @Override
  public CompressionFactory.LongEncodingReader duplicate()
  {
    return new DeltaLongEncodingReader(buffer.duplicate(), base, bitsPerValue);
  }
}
