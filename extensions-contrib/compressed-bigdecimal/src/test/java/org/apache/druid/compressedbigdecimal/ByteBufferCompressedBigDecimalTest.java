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

package org.apache.druid.compressedbigdecimal;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class ByteBufferCompressedBigDecimalTest
{
  private static final int SIZE = 4;
  private static final int SCALE = 2;

  private final ByteBuffer byteBuffer = ByteBuffer.allocate(100);

  @Test
  public void testInitZero()
  {
    ByteBufferCompressedBigDecimal.initZero(byteBuffer, 0, SIZE);

    testBufferValue("0");
  }

  @Test
  public void testInitMin()
  {
    ByteBufferCompressedBigDecimal.initMin(byteBuffer, 0, SIZE);

    testBufferValue("-1701411834604692317316873037158841057.28");
  }

  @Test
  public void testInitMax()
  {
    ByteBufferCompressedBigDecimal.initMax(byteBuffer, 0, SIZE);

    testBufferValue("1701411834604692317316873037158841057.27");
  }

  @Test
  public void testSetValue()
  {
    ByteBufferCompressedBigDecimal.initZero(byteBuffer, 0, SIZE);
    ByteBufferCompressedBigDecimal compressedBigDecimal = new ByteBufferCompressedBigDecimal(
        byteBuffer,
        0,
        SIZE,
        SCALE
    );
    String value = "10500.63";

    compressedBigDecimal.setValue(new ArrayCompressedBigDecimal(new BigDecimal(value)));

    testBufferValue(value);

  }

  public void testBufferValue(String expectedValue)
  {
    ByteBufferCompressedBigDecimal compressedBigDecimal = new ByteBufferCompressedBigDecimal(
        byteBuffer,
        0,
        SIZE,
        SCALE
    );

    Assert.assertEquals(expectedValue, compressedBigDecimal.toString());
  }
}
