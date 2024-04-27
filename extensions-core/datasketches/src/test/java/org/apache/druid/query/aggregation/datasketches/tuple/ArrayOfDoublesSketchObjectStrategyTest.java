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

package org.apache.druid.query.aggregation.datasketches.tuple;

import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ArrayOfDoublesSketchObjectStrategyTest
{
  @Test
  public void testSafeRead()
  {
    ArrayOfDoublesSketchObjectStrategy objectStrategy = new ArrayOfDoublesSketchObjectStrategy();
    ArrayOfDoublesUpdatableSketch sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(1024)
                                                                                     .setNumberOfValues(4)
                                                                                     .build();
    sketch.update(1L, new double[]{1.0, 2.0, 3.0, 4.0});

    final byte[] bytes = sketch.compact().toByteArray();

    ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    // valid sketch should not explode when copied, which reads the memory
    objectStrategy.fromByteBufferSafe(buf, bytes.length).compact().toByteArray();

    // corrupted sketch should fail with a regular java buffer exception
    for (int subset = 3; subset < bytes.length - 1; subset++) {
      final byte[] garbage2 = new byte[subset];
      for (int i = 0; i < garbage2.length; i++) {
        garbage2[i] = buf.get(i);
      }

      final ByteBuffer buf2 = ByteBuffer.wrap(garbage2).order(ByteOrder.LITTLE_ENDIAN);
      Assert.assertThrows(
          IndexOutOfBoundsException.class,
          () -> objectStrategy.fromByteBufferSafe(buf2, garbage2.length).compact().toByteArray()
      );
    }

    // non sketch that is too short to contain header should fail with regular java buffer exception
    final byte[] garbage = new byte[]{0x01, 0x02};
    final ByteBuffer buf3 = ByteBuffer.wrap(garbage).order(ByteOrder.LITTLE_ENDIAN);
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> objectStrategy.fromByteBufferSafe(buf3, garbage.length).compact().toByteArray()
    );
  }
}
