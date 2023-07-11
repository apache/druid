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

package org.apache.druid.segment.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ComparableIntArrayTest
{
  private final int[] array = new int[]{1, 2, 3};
  private final ComparableIntArray comparableIntArray = ComparableIntArray.of(1, 2, 3);

  @Test
  public void testDelegate()
  {
    Assert.assertArrayEquals(array, comparableIntArray.getDelegate());
    Assert.assertEquals(0, ComparableIntArray.of(new int[0]).getDelegate().length);
    Assert.assertEquals(0, ComparableIntArray.of().getDelegate().length);
  }

  @Test
  public void testHashCode()
  {
    Assert.assertEquals(Arrays.hashCode(array), comparableIntArray.hashCode());
    Set<ComparableIntArray> set = new HashSet<>();
    set.add(comparableIntArray);
    set.add(ComparableIntArray.of(array));
    Assert.assertEquals(1, set.size());
  }

  @Test
  public void testEquals()
  {
    Assert.assertTrue(comparableIntArray.equals(ComparableIntArray.of(array)));
    Assert.assertFalse(comparableIntArray.equals(ComparableIntArray.of(1, 2, 5)));
    Assert.assertFalse(comparableIntArray.equals(ComparableIntArray.EMPTY_ARRAY));
    Assert.assertFalse(comparableIntArray.equals(null));
  }

  @Test
  public void testCompareTo()
  {
    Assert.assertEquals(0, comparableIntArray.compareTo(ComparableIntArray.of(array)));
    Assert.assertEquals(1, comparableIntArray.compareTo(null));
    Assert.assertEquals(1, comparableIntArray.compareTo(ComparableIntArray.of(1, 2)));
    Assert.assertEquals(-1, comparableIntArray.compareTo(ComparableIntArray.of(1, 2, 3, 4)));
    Assert.assertTrue(comparableIntArray.compareTo(ComparableIntArray.of(2)) < 0);
  }
}
