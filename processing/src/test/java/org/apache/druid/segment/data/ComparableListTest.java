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

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComparableListTest
{
  private final List<Integer> integers = ImmutableList.of(1, 2, 3);
  private final ComparableList comparableList = new ComparableList(ImmutableList.of(1, 2, 3));

  @Test
  public void testDelegate()
  {
    Assert.assertEquals(integers, comparableList.getDelegate());
    Assert.assertEquals(0, new ComparableList(ImmutableList.of()).getDelegate().size());
  }

  @Test
  public void testHashCode()
  {
    Assert.assertEquals(integers.hashCode(), comparableList.hashCode());
    Set<ComparableList> set = new HashSet<>();
    set.add(comparableList);
    set.add(new ComparableList(integers));
    Assert.assertEquals(1, set.size());
  }

  @Test
  public void testEquals()
  {
    Assert.assertTrue(comparableList.equals(new ComparableList(integers)));
    Assert.assertFalse(comparableList.equals(new ComparableList(ImmutableList.of(1, 2, 5))));
    Assert.assertFalse(comparableList.equals(null));
  }

  @Test
  public void testCompareTo()
  {
    Assert.assertEquals(0, comparableList.compareTo(new ComparableList(integers)));
    Assert.assertEquals(1, comparableList.compareTo(null));
    Assert.assertEquals(1, comparableList.compareTo(new ComparableList(ImmutableList.of(1, 2))));
    Assert.assertEquals(-1, comparableList.compareTo(new ComparableList(ImmutableList.of(1, 2, 3, 4))));
    Assert.assertTrue(comparableList.compareTo(new ComparableList(ImmutableList.of(2))) < 0);
    ComparableList nullList = new ComparableList(new ArrayList<Integer>()
    {
      {
        add(null);
        add(1);
      }
    });

    Assert.assertTrue(comparableList.compareTo(nullList) > 0);
    Assert.assertTrue(nullList.compareTo(comparableList) < 0);
    Assert.assertTrue(nullList.compareTo(new ComparableList(new ArrayList<Integer>()
    {
      {
        add(null);
        add(1);
      }
    })) == 0);
  }
}
