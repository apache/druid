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
  public static <T> List<List<T>> makeSlicesStatic(final Iterator<T> iterator, final int numSlices)
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
   * Items are assigned to the slice that has the lowest weight at the time that item is encountered, which
   * leads to pseudo-round-robin assignment.
   */
  public static <T> List<List<T>> makeSlicesStatic(
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

  /**
   * Creates up to "maxNumSlices" lists from "iterator".
   *
   * This method creates as few slices as possible, while keeping each slice under the provided limits.
   *
   * This function uses a greedy algorithm that starts out with a single empty slice. Items are assigned to the slice
   * that has the lowest weight at the time that item is encountered. New slices are created, up to maxNumSlices, when
   * the lightest existing slice exceeds either maxFilesPerSlice or maxWeightPerSlice.
   *
   * Slices may have more than maxFilesPerSlice files, or more than maxWeightPerSlice weight, if necessary to
   * keep the total number of slices at or below maxNumSlices.
   */
  public static <T> List<List<T>> makeSlicesDynamic(
      final Iterator<T> iterator,
      final ToLongFunction<T> weightFunction,
      final int maxNumSlices,
      final int maxFilesPerSlice,
      final long maxWeightPerSlice
  )
  {
    final List<List<T>> slicesList = new ArrayList<>();
    final PriorityQueue<ListWithWeight<T>> pq = new PriorityQueue<>(
        // Break ties with position, so earlier slices fill first.
        Comparator.<ListWithWeight<T>, Long>comparing(ListWithWeight::getWeight)
                  .thenComparing(ListWithWeight::getPosition)
    );

    while (iterator.hasNext()) {
      final T item = iterator.next();
      final long itemWeight = weightFunction.applyAsLong(item);

      // Get lightest-weight list.
      ListWithWeight<T> lightestList = pq.poll();

      if (lightestList == null) {
        // Populate the initial list.
        lightestList = new ListWithWeight<>(new ArrayList<>(), 0, 0);
        slicesList.add(lightestList.getList());
      }

      if (!lightestList.getList().isEmpty()
          && slicesList.size() < maxNumSlices
          && (lightestList.getWeight() + itemWeight > maxWeightPerSlice
              || lightestList.getList().size() >= maxFilesPerSlice)) {
        // Lightest list can't hold this item without overflowing. Add it back and make another one.
        pq.add(lightestList);
        lightestList = new ListWithWeight<>(new ArrayList<>(), slicesList.size(), 0);
        slicesList.add(lightestList.getList());
      }

      lightestList.getList().add(item);
      pq.add(
          new ListWithWeight<>(
              lightestList.getList(),
              lightestList.getPosition(),
              lightestList.getWeight() + itemWeight
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
