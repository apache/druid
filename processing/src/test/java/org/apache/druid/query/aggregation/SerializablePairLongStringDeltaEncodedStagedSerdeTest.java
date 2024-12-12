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

import org.apache.druid.segment.serde.cell.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

public class SerializablePairLongStringDeltaEncodedStagedSerdeTest
{
  private static final SerializablePairLongStringDeltaEncodedStagedSerde INTEGER_SERDE =
      new SerializablePairLongStringDeltaEncodedStagedSerde(0L, true);

  private static final SerializablePairLongStringDeltaEncodedStagedSerde LONG_SERDE =
      new SerializablePairLongStringDeltaEncodedStagedSerde(0L, false);

  private final RandomStringUtils randomStringUtils = new RandomStringUtils();

  @Test
  public void testNull()
  {
    assertValueEquals(null, 0, INTEGER_SERDE);
  }

  @Test
  public void testSimpleInteger()
  {
    assertValueEquals(new SerializablePairLongString(100L, "fuu"), 11, INTEGER_SERDE);
  }

  @Test
  public void testNullStringInteger()
  {
    assertValueEquals(new SerializablePairLongString(100L, null), 8, INTEGER_SERDE);
  }

  @Test
  public void testEmptyStringInteger()
  {
    assertValueEquals(new SerializablePairLongString(100L, ""), 8, INTEGER_SERDE);
  }

  @Test
  public void testLargeStringInteger()
  {
    assertValueEquals(
        new SerializablePairLongString(100L, randomStringUtils.randomAlphanumeric(1024 * 1024)),
        1048584,
        INTEGER_SERDE
    );
  }

  @Test
  public void testSimpleLong()
  {
    assertValueEquals(new SerializablePairLongString(100L, "fuu"), 15, LONG_SERDE);
  }

  @Test
  public void testNullStringLong()
  {
    assertValueEquals(new SerializablePairLongString(100L, null), 12, LONG_SERDE);
  }

  @Test
  public void testEmptyStringLong()
  {
    assertValueEquals(new SerializablePairLongString(100L, ""), 12, LONG_SERDE);
  }


  @Test
  public void testLargeStringLong()
  {
    assertValueEquals(
        new SerializablePairLongString(100L, randomStringUtils.randomAlphanumeric(10 * 1024 * 1024)),
        10485772,
        LONG_SERDE
    );
  }

  private static void assertValueEquals(
      @Nullable SerializablePairLongString value,
      int size,
      SerializablePairLongStringDeltaEncodedStagedSerde serde
  )
  {
    byte[] bytes = serde.serialize(value);
    Assert.assertEquals(size, bytes.length);
    SerializablePairLongString deserialized = serde.deserialize(bytes);
    Assert.assertEquals(value, deserialized);
  }
}
