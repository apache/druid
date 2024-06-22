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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.serde.cell.RandomStringUtils;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class BackwardCompatibleSerializablePairLongStringDeltaEncodedStagedSerdeTest
{
  private static final OlderSerializablePairLongStringDeltaEncodedStagedSerde OLDER_INTEGER_SERDE =
      new OlderSerializablePairLongStringDeltaEncodedStagedSerde(0L, true);
  private static final SerializablePairLongStringDeltaEncodedStagedSerde INTEGER_SERDE =
      new SerializablePairLongStringDeltaEncodedStagedSerde(0L, true);

  private static final OlderSerializablePairLongStringDeltaEncodedStagedSerde OLDER_LONG_SERDE =
      new OlderSerializablePairLongStringDeltaEncodedStagedSerde(0L, false);
  private static final SerializablePairLongStringDeltaEncodedStagedSerde LONG_SERDE =
      new SerializablePairLongStringDeltaEncodedStagedSerde(0L, false);

  private static final Long TIMESTAMP = 100L;

  private final RandomStringUtils randomStringUtils = new RandomStringUtils(new Random(0));

  @Test
  public void testSimple()
  {
    SerializablePairLongString value = new SerializablePairLongString(TIMESTAMP, "fuu");
    testValue(value, OLDER_INTEGER_SERDE, INTEGER_SERDE);
    testValue(value, OLDER_LONG_SERDE, LONG_SERDE);
  }

  @Test
  public void testNull()
  {
    testValue(null, OLDER_INTEGER_SERDE, INTEGER_SERDE);
    testValue(null, OLDER_LONG_SERDE, LONG_SERDE);
  }

  @Test
  public void testNullString()
  {
    SerializablePairLongString value = new SerializablePairLongString(TIMESTAMP, null);

    // Write using the older serde, read using the newer serde
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, OLDER_INTEGER_SERDE), INTEGER_SERDE)
    );
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, OLDER_LONG_SERDE), LONG_SERDE)
    );

    // Write using the newer serde, read using the older serde
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, INTEGER_SERDE), OLDER_INTEGER_SERDE)
    );
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, LONG_SERDE), OLDER_LONG_SERDE)
    );

    // Compare the length of the serialized bytes for the value
    Assert.assertEquals(
        writeUsingSerde(value, OLDER_INTEGER_SERDE).length,
        writeUsingSerde(value, INTEGER_SERDE).length
    );
    Assert.assertEquals(writeUsingSerde(value, OLDER_LONG_SERDE).length, writeUsingSerde(value, LONG_SERDE).length);
  }

  @Test
  public void testEmptyString()
  {
    SerializablePairLongString value = new SerializablePairLongString(TIMESTAMP, "");

    // Write using the older serde, read using the newer serde
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, OLDER_INTEGER_SERDE), INTEGER_SERDE)
    );
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, OLDER_LONG_SERDE), LONG_SERDE)
    );

    // Write using the newer serde, read using the older serde
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, INTEGER_SERDE), OLDER_INTEGER_SERDE)
    );
    Assert.assertEquals(
        new SerializablePairLongString(TIMESTAMP, null),
        readUsingSerde(writeUsingSerde(value, LONG_SERDE), OLDER_LONG_SERDE)
    );

    // Compare the length of the serialized bytes for the value
    Assert.assertEquals(
        writeUsingSerde(value, OLDER_INTEGER_SERDE).length,
        writeUsingSerde(value, INTEGER_SERDE).length
    );
    Assert.assertEquals(writeUsingSerde(value, OLDER_LONG_SERDE).length, writeUsingSerde(value, LONG_SERDE).length);
  }

  @Test
  public void testLargeString()
  {
    SerializablePairLongString value = new SerializablePairLongString(
        TIMESTAMP,
        randomStringUtils.randomAlphanumeric(1024 * 1024)
    );
    testValue(value, OLDER_INTEGER_SERDE, INTEGER_SERDE);
    testValue(value, OLDER_LONG_SERDE, LONG_SERDE);
  }

  private void testValue(
      @Nullable SerializablePairLongString value,
      StagedSerde<SerializablePairLongString> olderSerde,
      StagedSerde<SerializablePairLongString> serde
  )
  {
    // Write using the older serde, read using the newer serde
    Assert.assertEquals(
        value,
        readUsingSerde(writeUsingSerde(value, olderSerde), serde)
    );
    // Write using the newer serde, read using the older serde
    Assert.assertEquals(
        value,
        readUsingSerde(writeUsingSerde(value, serde), olderSerde)
    );
    // Compare the length of the serialized bytes for the value
    Assert.assertEquals(writeUsingSerde(value, olderSerde).length, writeUsingSerde(value, serde).length);
  }

  private static byte[] writeUsingSerde(
      @Nullable SerializablePairLongString value,
      StagedSerde<SerializablePairLongString> serde
  )
  {
    return serde.serialize(value);
  }

  private static SerializablePairLongString readUsingSerde(
      byte[] bytes,
      StagedSerde<SerializablePairLongString> serde
  )
  {
    return serde.deserialize(bytes);
  }

  /**
   * Older serde class for delta encoded long-string pair serde, that treated empty and null strings equivalently and returned
   * coerced both to null
   */
  private static class OlderSerializablePairLongStringDeltaEncodedStagedSerde
      implements StagedSerde<SerializablePairLongString>
  {
    private final long minValue;
    private final boolean useIntegerDelta;

    public OlderSerializablePairLongStringDeltaEncodedStagedSerde(long minValue, boolean useIntegerDelta)
    {
      this.minValue = minValue;
      this.useIntegerDelta = useIntegerDelta;
    }

    @Override
    public StorableBuffer serializeDelayed(@Nullable SerializablePairLongString value)
    {
      if (value == null) {
        return StorableBuffer.EMPTY;
      }

      String rhsString = value.rhs;
      byte[] rhsBytes = StringUtils.toUtf8WithNullToEmpty(rhsString);

      return new StorableBuffer()
      {
        @Override
        public void store(ByteBuffer byteBuffer)
        {
          Preconditions.checkNotNull(value.lhs, "Long in SerializablePairLongString must be non-null");

          long delta = value.lhs - minValue;

          Preconditions.checkState(delta >= 0 || delta == value.lhs);

          if (useIntegerDelta) {
            byteBuffer.putInt(Ints.checkedCast(delta));
          } else {
            byteBuffer.putLong(delta);
          }

          byteBuffer.putInt(rhsBytes.length);

          if (rhsBytes.length > 0) {
            byteBuffer.put(rhsBytes);
          }
        }

        @Override
        public int getSerializedSize()
        {
          return (useIntegerDelta ? Integer.BYTES : Long.BYTES) + Integer.BYTES + rhsBytes.length;
        }
      };
    }

    @Nullable
    @Override
    public SerializablePairLongString deserialize(ByteBuffer byteBuffer)
    {
      if (byteBuffer.remaining() == 0) {
        return null;
      }

      ByteBuffer readOnlyBuffer = byteBuffer.asReadOnlyBuffer().order(ByteOrder.nativeOrder());
      long lhs;

      if (useIntegerDelta) {
        lhs = readOnlyBuffer.getInt();
      } else {
        lhs = readOnlyBuffer.getLong();
      }

      lhs += minValue;

      int stringSize = readOnlyBuffer.getInt();
      String lastString = null;

      if (stringSize > 0) {
        byte[] stringBytes = new byte[stringSize];

        readOnlyBuffer.get(stringBytes, 0, stringSize);
        lastString = StringUtils.fromUtf8(stringBytes);
      }

      return new SerializablePairLongString(lhs, lastString);
    }
  }
}
