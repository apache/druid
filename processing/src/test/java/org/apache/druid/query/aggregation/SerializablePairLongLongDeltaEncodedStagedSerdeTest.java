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

public class SerializablePairLongLongDeltaEncodedStagedSerdeTest
{
  private static final SerializablePairLongLongDeltaEncodedStagedSerde INTEGER_SERDE =
      new SerializablePairLongLongDeltaEncodedStagedSerde(0L, true);

  private static final SerializablePairLongLongDeltaEncodedStagedSerde LONG_SERDE =
      new SerializablePairLongLongDeltaEncodedStagedSerde(0L, false);

  private final Random random = new Random(0);

  @Test
  public void testNull()
  {
    assertValueEquals(null, 0, INTEGER_SERDE);
  }

  @Test
  public void testSimpleInteger()
  {
    assertValueEquals(new SerializablePairLongLong(100L, 10L), 13, INTEGER_SERDE);
  }

  @Test
  public void testNullRHSInteger()
  {
    assertValueEquals(new SerializablePairLongLong(100L, null), 5, INTEGER_SERDE);
  }

  @Test
  public void testLargeRHSInteger()
  {
    assertValueEquals(
        new SerializablePairLongLong(100L, random.nextLong()),
        13,
        INTEGER_SERDE
    );
  }

  @Test
  public void testSimpleLong()
  {
    assertValueEquals(new SerializablePairLongLong(100L, 10L), 17, LONG_SERDE);
  }

  @Test
  public void testNullRHSLong()
  {
    assertValueEquals(new SerializablePairLongLong(100L, null), 9, LONG_SERDE);
  }

  @Test
  public void testLargeRHSLong()
  {
    assertValueEquals(
        new SerializablePairLongLong(100L, random.nextLong()),
        17,
        LONG_SERDE
    );
  }

  private static void assertValueEquals(
      @Nullable SerializablePairLongLong value,
      int size,
      SerializablePairLongLongDeltaEncodedStagedSerde serde
  )
  {
    byte[] bytes = serde.serialize(value);
    Assert.assertEquals(size, bytes.length);
    SerializablePairLongLong deserialized = serde.deserialize(bytes);
    Assert.assertEquals(value, deserialized);
  }
}
