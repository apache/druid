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

package org.apache.druid.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class CircularBufferTest
{
  @Test
  public void testCircularBufferGetLatest()
  {
    CircularBuffer<Integer> buff = new CircularBuffer(4);

    for (int i = 1; i <= 9; i++) {
      buff.add(i); // buffer should contain [9, 6, 7, 8]
    }
    for (int i = 0; i < 4; i++) {
      Assertions.assertEquals((Integer) (9 - i), buff.getLatest(i));
    }
  }

  @Test
  public void testCircularBufferGet()
  {
    CircularBuffer<Integer> circularBuffer = new CircularBuffer<>(
        3);

    circularBuffer.add(1);
    Assertions.assertEquals(1, circularBuffer.size());
    Assertions.assertEquals(1, (int) circularBuffer.get(0));

    circularBuffer.add(2);
    Assertions.assertEquals(2, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 1, (int) circularBuffer.get(i));
    }

    circularBuffer.add(3);
    Assertions.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 1, (int) circularBuffer.get(i));
    }

    circularBuffer.add(4);
    Assertions.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 2, (int) circularBuffer.get(i));
    }

    circularBuffer.add(5);
    Assertions.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 3, (int) circularBuffer.get(i));
    }

    circularBuffer.add(6);
    Assertions.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 4, (int) circularBuffer.get(i));
    }

    circularBuffer.add(7);
    Assertions.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 5, (int) circularBuffer.get(i));
    }

    circularBuffer.add(8);
    Assertions.assertEquals(3, circularBuffer.size());
    for (int i = 0; i < circularBuffer.size(); i++) {
      Assertions.assertEquals(i + 6, (int) circularBuffer.get(i));
    }
  }
}
