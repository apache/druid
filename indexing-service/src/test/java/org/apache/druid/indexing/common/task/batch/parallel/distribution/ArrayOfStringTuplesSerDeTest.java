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
import org.apache.druid.data.input.StringTuple;
import org.junit.Assert;
import org.junit.Test;

public class ArrayOfStringTuplesSerDeTest
{

  private final ArrayOfStringTuplesSerDe serde = new ArrayOfStringTuplesSerDe();

  @Test
  public void testStringTupleSerde()
  {
    testSerde(StringTuple.create("abc"));
    testSerde(StringTuple.create("abc", "def", "xyz"));
    testSerde(new StringTuple[]{StringTuple.create("abc"), StringTuple.create("def", "efg"), StringTuple.create("z")});
  }

  @Test
  public void testEmptyTuple()
  {
    testSerde(StringTuple.create());
    testSerde(new StringTuple[]{});
  }

  @Test
  public void testArrayWithNullAndEmptyString()
  {
    testSerde(StringTuple.create(""));
    testSerde(StringTuple.create("abc", "def", ""));
    testSerde(StringTuple.create("abc", null, "def"));
    testSerde(new StringTuple[]{StringTuple.create(null, null), StringTuple.create(null, null)});
    testSerde(new StringTuple[]{StringTuple.create("", ""), StringTuple.create("")});
    testSerde(StringTuple.create("", null, "abc"));
  }

  @Test
  public void testSizeOf()
  {
    StringTuple stringTuple = StringTuple.create("a", "b");
    Assert.assertEquals(serde.sizeOf(stringTuple), serde.serializeToByteArray(stringTuple).length);
  }

  private void testSerde(StringTuple stringTuple)
  {
    byte[] bytes = serde.serializeToByteArray(stringTuple);
    Assert.assertEquals(serde.sizeOf(stringTuple), bytes.length);

    Memory wrappedMemory = Memory.wrap(bytes);
    Assert.assertEquals(serde.sizeOf(wrappedMemory, 0, 1), bytes.length);

    StringTuple[] deserialized = serde.deserializeFromMemory(wrappedMemory, 1);
    Assert.assertArrayEquals(new StringTuple[]{stringTuple}, deserialized);
  }

  private void testSerde(StringTuple[] inputArray)
  {
    byte[] bytes = serde.serializeToByteArray(inputArray);
    Assert.assertEquals(serde.sizeOf(inputArray), bytes.length);

    Memory wrappedMemory = Memory.wrap(bytes);
    Assert.assertEquals(serde.sizeOf(wrappedMemory, 0, inputArray.length), bytes.length);

    StringTuple[] deserialized = serde.deserializeFromMemory(wrappedMemory, inputArray.length);
    Assert.assertArrayEquals(inputArray, deserialized);
  }

}
