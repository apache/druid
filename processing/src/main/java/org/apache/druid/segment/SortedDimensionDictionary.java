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

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2IntSortedMap;
import org.apache.druid.java.util.common.guava.Comparators;

import java.util.List;

/**
 * Creates a sorted dictionary given some existing dictionary, storing a mapping of both sorted id to unsorted id
 * and unsorted id to sorted id
 */
public class SortedDimensionDictionary<T extends Comparable<T>>
{
  private final List<T> sortedVals;
  private final int[] idToIndex;
  private final int[] indexToId;

  public SortedDimensionDictionary(List<T> idToValue, int length)
  {
    Object2IntSortedMap<T> sortedMap = new Object2IntRBTreeMap<>(Comparators.naturalNullsFirst());
    for (int id = 0; id < length; id++) {
      T value = idToValue.get(id);
      sortedMap.put(value, id);
    }
    this.sortedVals = Lists.newArrayList(sortedMap.keySet());
    this.idToIndex = new int[length];
    this.indexToId = new int[length];
    int index = 0;
    for (IntIterator iterator = sortedMap.values().iterator(); iterator.hasNext(); ) {
      int id = iterator.nextInt();
      idToIndex[id] = index;
      indexToId[index] = id;
      index++;
    }
  }

  public int getUnsortedIdFromSortedId(int index)
  {
    return indexToId[index];
  }

  public int getSortedIdFromUnsortedId(int id)
  {
    return idToIndex[id];
  }

  public T getValueFromSortedId(int index)
  {
    return sortedVals.get(index);
  }
}
