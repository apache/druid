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

package org.apache.druid.query.aggregation;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.segment.serde.cell.LongSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Locale;

public abstract class AbstractSerializablePairLongObjectColumnHeader<T extends SerializablePair<Long, ?>>
{
  // header size is 4 bytes for word alignment for LZ4 (minmatch) compression
  private static final int HEADER_SIZE_BYTES = 4;
  private static final int USE_INTEGER_MASK = 0x80;
  private static final int VERSION_INDEX = 0;
  private static final int ENCODING_INDEX = 1;

  private final byte[] bytes;
  final long minValue;

  AbstractSerializablePairLongObjectColumnHeader(byte[] bytes, long minValue)
  {
    this.bytes = bytes;
    this.minValue = minValue;
  }

  AbstractSerializablePairLongObjectColumnHeader(int version, boolean useIntegerDeltas, long minTimestamp)
  {
    this.minValue = minTimestamp;
    bytes = new byte[HEADER_SIZE_BYTES];
    Preconditions.checkArgument(version <= 255, "max version 255");
    bytes[VERSION_INDEX] = (byte) version;

    if (useIntegerDeltas) {
      bytes[ENCODING_INDEX] |= USE_INTEGER_MASK;
    }
  }

  static AbstractSerializablePairLongObjectColumnHeader<?> fromBuffer(ByteBuffer byteBuffer, Class<?> pairClass)
  {
    byte[] bytes = new byte[HEADER_SIZE_BYTES];
    byteBuffer.get(bytes);
    long minTimestamp = byteBuffer.getLong();

    if (pairClass.isAssignableFrom(SerializablePairLongLong.class)) {
      return new SerializablePairLongLongColumnHeader(bytes, minTimestamp);
    }

    if (pairClass.isAssignableFrom(SerializablePairLongDouble.class)) {
      return new SerializablePairLongDoubleColumnHeader(bytes, minTimestamp);
    }

    if (pairClass.isAssignableFrom(SerializablePairLongFloat.class)) {
      return new SerializablePairLongFloatColumnHeader(bytes, minTimestamp);
    }

    if (pairClass.isAssignableFrom(SerializablePairLongString.class)) {
      return new SerializablePairLongStringColumnHeader(bytes, minTimestamp);
    }

    throw new RE(String.format(Locale.ENGLISH, "Unsupported pairClass type: %s", pairClass.getSimpleName()));
  }

  public abstract AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<?> createSerde();

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    LongSerializer longSerializer = new LongSerializer();

    channel.write(ByteBuffer.wrap(bytes));
    channel.write(longSerializer.serialize(minValue));
  }

  public int getVersion()
  {
    return 0XFF & bytes[VERSION_INDEX];
  }

  public boolean isUseIntegerDeltas()
  {
    return (bytes[ENCODING_INDEX] & USE_INTEGER_MASK) != 0;
  }

  public long getMinValue()
  {
    return minValue;
  }

  public int getSerializedSize()
  {
    return HEADER_SIZE_BYTES + Long.BYTES;
  }

  @Override
  public String toString()
  {
    return MoreObjects.toStringHelper(this)
                      .add("bytes", bytes)
                      .add("minValue", minValue)
                      .toString();
  }
}
