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

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Random;

public class SerializablePairLongFloatDeltaEncodedStagedSerdeTest
{
  private static final SerializablePairLongFloatDeltaEncodedStagedSerde INTEGER_SERDE =
      new SerializablePairLongFloatDeltaEncodedStagedSerde(0L, true);

  private static final SerializablePairLongFloatDeltaEncodedStagedSerde LONG_SERDE =
      new SerializablePairLongFloatDeltaEncodedStagedSerde(0L, false);

  private final Random random = new Random(0);

  @Test
  public void testNull()
  {
    assertValueEquals(null, 0, INTEGER_SERDE);
  }

  @Test
  public void testSimpleInteger()
  {
    assertValueEquals(new SerializablePairLongFloat(100L, 10F), 9, INTEGER_SERDE);
  }

  @Test
  public void testNullRHSInteger()
  {
    assertValueEquals(new SerializablePairLongFloat(100L, null), 5, INTEGER_SERDE);
  }

  @Test
  public void testLargeRHSInteger()
  {
    assertValueEquals(
        new SerializablePairLongFloat(100L, random.nextFloat()),
        9,
        INTEGER_SERDE
    );
  }

  @Test
  public void testSimpleLong()
  {
    assertValueEquals(new SerializablePairLongFloat(100L, 10F), 13, LONG_SERDE);
  }

  @Test
  public void testNullRHSLong()
  {
    assertValueEquals(new SerializablePairLongFloat(100L, null), 9, LONG_SERDE);
  }

  @Test
  public void testLargeRHSLong()
  {
    assertValueEquals(
        new SerializablePairLongFloat(100L, random.nextFloat()),
        13,
        LONG_SERDE
    );
  }

  private static void assertValueEquals(
      @Nullable SerializablePairLongFloat value,
      int size,
      SerializablePairLongFloatDeltaEncodedStagedSerde serde
  )
  {
    byte[] bytes = serde.serialize(value);
    Assert.assertEquals(size, bytes.length);
    SerializablePairLongFloat deserialized = serde.deserialize(bytes);
    Assert.assertEquals(value, deserialized);
  }
}
