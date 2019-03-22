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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class CircularBufferTest
{
  private static CircularBuffer<Integer> buff = new CircularBuffer(4);

  @BeforeClass
  public static void setup()
  {
    for (int i = 1; i <= 9; i++) {
      buff.add(i); // buffer should contain [9, 6, 7, 8]
    }
  }

  @Test
  public void testCircularBufferGetLatest()
  {
    for (int i = 0; i < 4; i++) {
      Assert.assertEquals((Integer) (9 - i), buff.getLatest(i));
    }
  }

  @Test
  public void testCircularBufferGet()
  {
    for (int i = 0; i < 4; i++) {
      Assert.assertEquals((Integer) (i + 6), buff.get(i));
    }
  }

  @Test
  public void testCircularBufferToList()
  {
    List<Integer> buffToList = buff.toList();
    Assert.assertEquals(4, buffToList.size());
    for (int i = 6; i <= 9; i++) {
      Assert.assertEquals((Integer) i, buffToList.get(i - 6));
    }
  }
}