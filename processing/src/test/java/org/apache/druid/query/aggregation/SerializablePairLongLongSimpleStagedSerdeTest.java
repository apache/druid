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

public class SerializablePairLongLongSimpleStagedSerdeTest
{
  private static final SerializablePairLongLongSimpleStagedSerde SERDE =
      new SerializablePairLongLongSimpleStagedSerde();

  private final Random random = new Random(0);

  @Test
  public void testSimple()
  {
    assertValueEquals(new SerializablePairLongLong(Long.MAX_VALUE, 10L), 17);
  }

  @Test
  public void testNull()
  {
    assertValueEquals(null, 0);
  }

  @Test
  public void testNullString()
  {
    assertValueEquals(new SerializablePairLongLong(Long.MAX_VALUE, null), 9);
  }

  @Test
  public void testLargeRHS()
  {
    assertValueEquals(
        new SerializablePairLongLong(Long.MAX_VALUE, random.nextLong()),
        17
    );
  }

  private static void assertValueEquals(@Nullable SerializablePairLongLong value, int size)
  {
    byte[] bytes = SERDE.serialize(value);
    Assert.assertEquals(size, bytes.length);
    SerializablePairLongLong deserialized = SERDE.deserialize(bytes);
    Assert.assertEquals(value, deserialized);
  }
}
