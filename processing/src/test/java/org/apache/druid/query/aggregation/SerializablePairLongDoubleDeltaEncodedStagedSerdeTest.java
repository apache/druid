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

public class SerializablePairLongDoubleDeltaEncodedStagedSerdeTest
{
  private static final SerializablePairLongDoubleDeltaEncodedStagedSerde INTEGER_SERDE =
      new SerializablePairLongDoubleDeltaEncodedStagedSerde(0L, true);

  private static final SerializablePairLongDoubleDeltaEncodedStagedSerde LONG_SERDE =
      new SerializablePairLongDoubleDeltaEncodedStagedSerde(0L, false);

  private final Random random = new Random(0);

  @Test
  public void testNull()
  {
    assertValueEquals(null, 0, INTEGER_SERDE);
  }

  @Test
  public void testSimpleInteger()
  {
    assertValueEquals(new SerializablePairLongDouble(100L, 1000000000000.12312312312D), 13, INTEGER_SERDE);
  }

  @Test
  public void testNullRHSInteger()
  {
    assertValueEquals(new SerializablePairLongDouble(100L, null), 5, INTEGER_SERDE);
  }

  @Test
  public void testLargeRHSInteger()
  {
    assertValueEquals(
        new SerializablePairLongDouble(100L, random.nextDouble()),
        13,
        INTEGER_SERDE
    );
  }

  @Test
  public void testSimpleLong()
  {
    assertValueEquals(new SerializablePairLongDouble(100L, 1000000000000.12312312312D), 17, LONG_SERDE);
  }

  @Test
  public void testNullRHSLong()
  {
    assertValueEquals(new SerializablePairLongDouble(100L, null), 9, LONG_SERDE);
  }

  @Test
  public void testLargeRHSLong()
  {
    assertValueEquals(
        new SerializablePairLongDouble(100L, random.nextDouble()),
        17,
        LONG_SERDE
    );
  }

  private static void assertValueEquals(
      @Nullable SerializablePairLongDouble value,
      int size,
      SerializablePairLongDoubleDeltaEncodedStagedSerde serde
  )
  {
    byte[] bytes = serde.serialize(value);
    Assert.assertEquals(size, bytes.length);
    SerializablePairLongDouble deserialized = serde.deserialize(bytes);
    Assert.assertEquals(value, deserialized);
  }
}
