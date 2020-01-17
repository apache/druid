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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class StringFirstLastUtilsTest
{
  private static final SerializablePairLongString PAIR_TO_WRITE = new SerializablePairLongString(
      DateTimes.MAX.getMillis(),
      "asdasddsaasd"
  );

  private static final int BUFFER_CAPACITY = 100;
  // PAIR_TO_WRITE Size is 12 so MAX_BYTE_TO_WRITE is set to 15 which is more than enough
  private static final int MAX_BYTE_TO_WRITE = 15;

  @Test
  public void testWritePairThenReadPairAtBeginningBuffer()
  {
    int positionAtBeginning = 0;
    ByteBuffer buf = ByteBuffer.allocate(BUFFER_CAPACITY);
    StringFirstLastUtils.writePair(buf, positionAtBeginning, PAIR_TO_WRITE, MAX_BYTE_TO_WRITE);
    SerializablePairLongString actual = StringFirstLastUtils.readPair(buf, positionAtBeginning);
    Assert.assertEquals(PAIR_TO_WRITE, actual);
  }

  @Test
  public void testWritePairThenReadPairAtMiddleBuffer()
  {
    int positionAtMiddle = 60;
    ByteBuffer buf = ByteBuffer.allocate(BUFFER_CAPACITY);
    StringFirstLastUtils.writePair(buf, positionAtMiddle, PAIR_TO_WRITE, MAX_BYTE_TO_WRITE);
    SerializablePairLongString actual = StringFirstLastUtils.readPair(buf, positionAtMiddle);
    Assert.assertEquals(PAIR_TO_WRITE, actual);
  }
}
