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

package org.apache.druid.segment;

import it.unimi.dsi.fastutil.ints.IntList;
import org.junit.Assert;
import org.junit.Test;

public class IntListUtilsTest
{
  @Test(expected = IndexOutOfBoundsException.class)
  public void testEmptyRangeIntList()
  {
    final IntList list = IntListUtils.fromTo(10, 10);
    Assert.assertEquals(0, list.size());
    list.getInt(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRangeIntListWithSmallEndIndex()
  {
    IntListUtils.fromTo(10, 5);
  }

  @Test
  public void testRangeIntList()
  {
    final IntList list = IntListUtils.fromTo(20, 120);
    for (int i = 0; i < 100; i++) {
      Assert.assertEquals(i + 20, list.getInt(i));
    }
  }
}
