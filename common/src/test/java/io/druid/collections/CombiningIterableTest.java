/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.collections;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Iterables;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;


import java.util.*;

public class CombiningIterableTest
{
  @Test
  public void testCreateSplatted()
  {
    List<Integer> firstList = Arrays.asList(1, 2, 5, 7, 9, 10, 20);
    List<Integer> secondList = Arrays.asList(1, 2, 5, 8, 9);
    Set<Integer> mergedLists = new HashSet<>();
    mergedLists.addAll(firstList);
    mergedLists.addAll(secondList);
    ArrayList<Iterable<Integer>> iterators = Lists.newArrayList();
    iterators.add(firstList);
    iterators.add(secondList);
    CombiningIterable<Integer> actualIterable = CombiningIterable.createSplatted(
        iterators,
        Ordering.<Integer>natural()
    );
    Assert.assertEquals(mergedLists.size(),Iterables.size(actualIterable));
    Set actualHashset = Sets.newHashSet(actualIterable);
    Assert.assertEquals(actualHashset,mergedLists);
  }
}
