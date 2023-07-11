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

package org.apache.druid.query.aggregation.datasketches.theta;

import org.apache.datasketches.Family;
import org.apache.datasketches.SketchesArgumentException;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SketchHolderObjectStrategyTest
{
  @Test
  public void testSafeRead()
  {
    SketchHolderObjectStrategy objectStrategy = new SketchHolderObjectStrategy();
    Union union = (Union) SetOperation.builder().setNominalEntries(1024).build(Family.UNION);
    union.update(1234L);

    final byte[] bytes = union.getResult().toByteArray();

    ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

    // valid sketch should not explode when copied, which reads the memory
    objectStrategy.fromByteBufferSafe(buf, bytes.length).getSketch().compact().getCompactBytes();

    // corrupted sketch should fail with a regular java buffer exception
    for (int subset = 3; subset < bytes.length - 1; subset++) {
      final byte[] garbage2 = new byte[subset];
      for (int i = 0; i < garbage2.length; i++) {
        garbage2[i] = buf.get(i);
      }

      final ByteBuffer buf2 = ByteBuffer.wrap(garbage2).order(ByteOrder.LITTLE_ENDIAN);
      Assert.assertThrows(
          IndexOutOfBoundsException.class,
          () -> objectStrategy.fromByteBufferSafe(buf2, garbage2.length).getSketch().compact().getCompactBytes()
      );
    }

    // non sketch that is too short to contain header should fail with regular java buffer exception
    final byte[] garbage = new byte[]{0x01, 0x02};
    final ByteBuffer buf3 = ByteBuffer.wrap(garbage).order(ByteOrder.LITTLE_ENDIAN);
    Assert.assertThrows(
        IndexOutOfBoundsException.class,
        () -> objectStrategy.fromByteBufferSafe(buf3, garbage.length).getSketch().compact().getCompactBytes()
    );

    // non sketch that is long enough to check (this one doesn't actually need 'safe' read)
    final byte[] garbageLonger = StringUtils.toUtf8("notasketch");
    final ByteBuffer buf4 = ByteBuffer.wrap(garbageLonger).order(ByteOrder.LITTLE_ENDIAN);
    Assert.assertThrows(
        SketchesArgumentException.class,
        () -> objectStrategy.fromByteBufferSafe(buf4, garbageLonger.length).getSketch().compact().getCompactBytes()
    );
  }
}
