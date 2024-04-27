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
import java.util.Random;

public class SerializablePairLongStringSimpleStagedSerdeTest
{
  private static final SerializablePairLongStringSimpleStagedSerde SERDE =
      new SerializablePairLongStringSimpleStagedSerde();

  private final RandomStringUtils randomStringUtils = new RandomStringUtils(new Random(0));

  @Test
  public void testSimple()
  {
    assertValueEquals(new SerializablePairLongString(Long.MAX_VALUE, "fuu"), 15);
  }

  @Test
  public void testNull()
  {
    assertValueEquals(null, 0);
  }

  @Test
  public void testNullString()
  {
    assertValueEquals(new SerializablePairLongString(Long.MAX_VALUE, null), 12);
  }

  @Test
  public void testEmptyString()
  {
    assertValueEquals(new SerializablePairLongString(Long.MAX_VALUE, ""), 12);
  }

  @Test
  public void testLargeString()
  {
    assertValueEquals(
        new SerializablePairLongString(Long.MAX_VALUE, randomStringUtils.randomAlphanumeric(1024 * 1024)),
        1048588
    );
  }

  private static void assertValueEquals(@Nullable SerializablePairLongString value, int size)
  {
    byte[] bytes = SERDE.serialize(value);
    Assert.assertEquals(size, bytes.length);
    SerializablePairLongString deserialized = SERDE.deserialize(bytes);
    Assert.assertEquals(value, deserialized);
  }
}
