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

package org.apache.druid.msq.input;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.ToLongFunction;

public class SlicerUtils
{

  /**
   * Creates "numSlices" lists from "iterator", trying to keep each one as evenly-sized as possible. Some lists may
   * be empty.
   *
   * Items are assigned round-robin.
   */
  public static <T> List<List<T>> makeSlices(final Iterator<T> iterator, final int numSlices)
  {
    final List<List<T>> slicesList = new ArrayList<>(numSlices);

    while (slicesList.size() < numSlices) {
      slicesList.add(new ArrayList<>());
    }

    int i = 0;
    while (iterator.hasNext()) {
      final T obj = iterator.next();
      slicesList.get(i % numSlices).add(obj);
      i++;
    }

    return slicesList;
  }

  /**
   * Creates "numSlices" lists from "iterator", trying to keep each one as evenly-weighted as possible. Some lists may
   * be empty.
   *
   * Each item is assigned to the split list that has the lowest weight at the time that item is encountered, which
   * leads to pseudo-round-robin assignment.
   */
  public static <T> List<List<T>> makeSlices(
      final Iterator<T> iterator,
      final ToLongFunction<T> weightFunction,
      final int numSlices
  )
  {
    final List<List<T>> slicesList = new ArrayList<>(numSlices);
    final PriorityQueue<ListWithWeight<T>> pq = new PriorityQueue<>(
        numSlices,

        // Break ties with position, so earlier slices fill first.
        Comparator.<ListWithWeight<T>, Long>comparing(ListWithWeight::getWeight)
                  .thenComparing(ListWithWeight::getPosition)
    );

    while (slicesList.size() < numSlices) {
      final ArrayList<T> list = new ArrayList<>();
      pq.add(new ListWithWeight<>(list, pq.size(), 0));
      slicesList.add(list);
    }

    while (iterator.hasNext()) {
      final T obj = iterator.next();
      final long itemWeight = weightFunction.applyAsLong(obj);
      final ListWithWeight<T> listWithWeight = pq.remove();
      listWithWeight.getList().add(obj);
      pq.add(
          new ListWithWeight<>(
              listWithWeight.getList(),
              listWithWeight.getPosition(),
              listWithWeight.getWeight() + itemWeight
          )
      );
    }

    return slicesList;
  }

  private static class ListWithWeight<T>
  {
    private final List<T> list;
    private final int position;
    private final long totalSize;

    public ListWithWeight(List<T> list, int position, long totalSize)
    {
      this.list = list;
      this.position = position;
      this.totalSize = totalSize;
    }

    public List<T> getList()
    {
      return list;
    }

    public int getPosition()
    {
      return position;
    }

    public long getWeight()
    {
      return totalSize;
    }
  }
}
