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

package org.apache.druid.collections.fastutil;

import org.junit.Assert;
import org.junit.Test;

public class DruidIntListTest
{

  @Test
  public void addArray()
  {
    DruidIntList intList = new DruidIntList(2);

    final int[] arrrrr = {0, 1, 2, 3};
    intList.addArray(arrrrr);
    expectEquals(intList, arrrrr);
  }

  @Test
  public void fill()
  {
    DruidIntList intList = new DruidIntList(2);

    intList.fill(2, 4);

    expectEquals(intList, new int[]{2, 2, 2, 2});
  }

  @Test
  public void fillWithRepeat()
  {
    DruidIntList intList = new DruidIntList(2);

    intList.fillWithRepeat(new int[]{0, 1}, 2);

    expectEquals(intList, new int[]{0, 1, 0, 1});
  }

  @Test
  public void fillRuns()
  {
    DruidIntList intList = new DruidIntList(2);

    intList.fillRuns(new int[]{0, 1}, 2, 2);

    expectEquals(intList, new int[]{0, 0, 1, 1, 0, 0, 1, 1});
  }

  @Test
  public void fillRunsRunLengthOne()
  {
    DruidIntList intList = new DruidIntList(2);

    intList.fillRuns(new int[]{0, 1}, 1, 2);

    expectEquals(intList, new int[]{0, 1, 0, 1});
  }

  @Test
  public void resetToSize()
  {
    DruidIntList intList = new DruidIntList(2);

    intList.fill(2, 4);

    Assert.assertEquals(4, intList.size());
    intList.resetToSize(4);
    Assert.assertEquals(0, intList.size());
  }

  private void expectEquals(DruidIntList intList, int[] expected)
  {
    Assert.assertEquals(expected.length, intList.size());
    for (int i = 0; i < expected.length; ++i) {
      Assert.assertEquals(String.valueOf(i), expected[i], intList.getInt(i));
    }
  }
}
