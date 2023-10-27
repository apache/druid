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

package org.apache.druid.msq.statistics;

import org.apache.datasketches.memory.Memory;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class ByteRowKeySerdeTest extends InitializedNullHandlingTest
{
  private final QuantilesSketchKeyCollectorFactory.ByteRowKeySerde serde =
      QuantilesSketchKeyCollectorFactory.ByteRowKeySerde.INSTANCE;

  @Test
  public void testByteArraySerde()
  {
    testSerde(new byte[]{1, 5, 9, 3});
    testSerde(new byte[][]{new byte[]{1, 5}, new byte[]{2, 3}, new byte[]{6, 7}});
  }

  @Test
  public void testSerdeWithRowKeys()
  {
    RowSignature rowSignature = RowSignature.builder()
                                            .add("x", ColumnType.LONG)
                                            .add("y", ColumnType.LONG)
                                            .build();

    testSerde(KeyTestUtils.createKey(rowSignature, 2, 4).array());
  }

  @Test
  public void testEmptyArray()
  {
    testSerde(new byte[][]{});
    testSerde(new byte[][]{new byte[]{1, 5}, new byte[]{}, new byte[]{2, 3}});
  }

  private void testSerde(byte[] byteRowKey)
  {
    byte[] bytes = serde.serializeToByteArray(byteRowKey);
    Assert.assertEquals(serde.sizeOf(byteRowKey), bytes.length);

    Memory wrappedMemory = Memory.wrap(bytes);
    Assert.assertEquals(serde.sizeOf(wrappedMemory, 0, 1), bytes.length);

    byte[][] deserialized = serde.deserializeFromMemory(wrappedMemory, 1);
    Assert.assertArrayEquals(new byte[][]{byteRowKey}, deserialized);
  }

  private void testSerde(byte[][] inputArray)
  {
    byte[] bytes = serde.serializeToByteArray(inputArray);
    Assert.assertEquals(serde.sizeOf(inputArray), bytes.length);

    Memory wrappedMemory = Memory.wrap(bytes);
    Assert.assertEquals(serde.sizeOf(wrappedMemory, 0, inputArray.length), bytes.length);

    byte[][] deserialized = serde.deserializeFromMemory(wrappedMemory, inputArray.length);
    Assert.assertArrayEquals(inputArray, deserialized);
  }
}
