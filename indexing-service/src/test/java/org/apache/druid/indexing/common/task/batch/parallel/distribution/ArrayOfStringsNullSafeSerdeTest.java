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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.IAE;
import org.junit.Assert;
import org.junit.Test;

public class ArrayOfStringsNullSafeSerdeTest
{

  private final ArrayOfStringsNullSafeSerde serde = new ArrayOfStringsNullSafeSerde();

  @Test
  public void testStringArray()
  {
    testSerde("abc", "def", "xyz");
    testSerde("abc", "123", "456.0");
  }

  @Test
  public void testSingletonArray()
  {
    testSerde("abc");
    testSerde("xyz");
  }

  @Test
  public void testEmptyArray()
  {
    testSerde();
  }

  @Test
  public void testArrayWithNullString()
  {
    testSerde((String) null);
    testSerde("abc", null, "def");
    testSerde(null, null, null);
  }

  @Test
  public void testArrayWithEmptyString()
  {
    testSerde("");
    testSerde("abc", "def", "");
    testSerde("", "", "");
    testSerde("", null, "abc");
  }

  @Test
  public void testIllegalStrLength()
  {
    // bytes for length = -2
    final byte[] bytes = {-2, -1, -1, -1};
    IAE exception = Assert.assertThrows(
        IAE.class,
        () -> serde.deserializeFromMemory(Memory.wrap(bytes), 1)
    );
    Assert.assertEquals(
        "Illegal strLength [-2] at offset [4]. Must be -1, 0 or a positive integer.",
        exception.getMessage()
    );
  }

  private void testSerde(String... inputArray)
  {
    byte[] bytes = serde.serializeToByteArray(inputArray);
    String[] deserialized = serde.deserializeFromMemory(Memory.wrap(bytes), inputArray.length);
    Assert.assertEquals(inputArray, deserialized);
  }

}
