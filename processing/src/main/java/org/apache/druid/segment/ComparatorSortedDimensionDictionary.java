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

import it.unimi.dsi.fastutil.ints.IntArrays;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Creates a sorted dictionary given some existing dictionary and a {@link Comparator}, storing a mapping of both
 * sorted id to unsorted id and unsorted id to sorted id. {@link SortedDimensionDictionary} uses natural ordering.
 */
public class ComparatorSortedDimensionDictionary<T>
{
  private final List<T> sortedVals;
  private final int[] idToIndex;
  private final int[] indexToId;

  /**
   * @param idToValue  unsorted dictionary values, which must be unique. The sort looks up values by id many
   *                   times, so this should be a {@link java.util.RandomAccess} list such as {@link ArrayList}.
   * @param comparator ordering of the values, which must handle null
   * @param length     number of ids, from the start of {@code idToValue}, to include
   */
  public ComparatorSortedDimensionDictionary(List<T> idToValue, Comparator<T> comparator, int length)
  {
    final int[] sortedIds = new int[length];
    for (int id = 0; id < length; id++) {
      sortedIds[id] = id;
    }
    IntArrays.quickSort(sortedIds, (id1, id2) -> comparator.compare(idToValue.get(id1), idToValue.get(id2)));

    this.sortedVals = new ArrayList<>(length);
    this.idToIndex = new int[length];
    this.indexToId = sortedIds;
    for (int index = 0; index < length; index++) {
      final int id = sortedIds[index];
      sortedVals.add(idToValue.get(id));
      idToIndex[id] = index;
    }
  }

  /**
   * Number of ids in this sorted view, captured at {@link DimensionDictionary#sort()} time. Both the sorted-id and
   * unsorted-id spaces cover {@code [0, size())}. Useful when iterating a snapshot taken concurrently with a still-
   * growing source dictionary, where re-reading the source size would be inconsistent with this view.
   */
  public int size()
  {
    return sortedVals.size();
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
