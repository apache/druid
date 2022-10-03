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

package org.apache.druid.collections;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.scan.ScanQuery;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public class QueueBasedMultiColumnSorterTests
{
  @Test
  public void singleColumnAscSort()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = getMultiColumnSorterElementComparator(orderByDirection);
    QueueBasedMultiColumnSorter queueBasedMultiColumnSorter = new QueueBasedMultiColumnSorter(5, comparator);
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(2, ImmutableList.of(2)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(3, ImmutableList.of(3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(4, ImmutableList.of(4)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(5, ImmutableList.of(5)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(6, ImmutableList.of(7)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(7, ImmutableList.of(8)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(9)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(100, ImmutableList.of(0)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(9, ImmutableList.of(6)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(11, ImmutableList.of(6)));
    Iterator<Integer> it = queueBasedMultiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(100, 1, 1, 2, 1);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }

  @Test
  public void singleColumnAscSortNaturalNullsFirst()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = getMultiColumnSorterElementComparator(orderByDirection);
    QueueBasedMultiColumnSorter queueBasedMultiColumnSorter = new QueueBasedMultiColumnSorter(5, comparator);
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(2, ImmutableList.of(2)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(3, ImmutableList.of(3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(4, ImmutableList.of(4)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(5, ImmutableList.of(5)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(6, ImmutableList.of(7)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(7, ImmutableList.of(8)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(9)));
    List list = new ArrayList();
    list.add(null);
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(100, list));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(9, ImmutableList.of(6)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(11, ImmutableList.of(6)));
    Iterator<Integer> it = queueBasedMultiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(100, 1, 1, 2, 1);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }

  @Nonnull
  private Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> getMultiColumnSorterElementComparator(List<String> orderByDirection)
  {
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Integer> o1,
          MultiColumnSorter.MultiColumnSorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!Objects.equals(o1.getOrderByColumValues().get(i), o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return Comparators.<Comparable>naturalNullsFirst().compare(o1.getOrderByColumValues().get(i), o2.getOrderByColumValues().get(i));
            } else {
              return Comparators.<Comparable>naturalNullsFirst().compare(o2.getOrderByColumValues().get(i), o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    return comparator;
  }

  @Test
  public void multiColumnSort()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = getMultiColumnSorterElementComparator(orderByDirection);
    QueueBasedMultiColumnSorter queueBasedMultiColumnSorter = new QueueBasedMultiColumnSorter(5, comparator);
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(2, ImmutableList.of(0, 0, 2)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(3, ImmutableList.of(0, 0, 3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(4, ImmutableList.of(0, 0, 4)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(5, ImmutableList.of(0, 3, 5)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(6, ImmutableList.of(0, 6, 7)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(7, ImmutableList.of(0, 0, 8)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 9)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(100, ImmutableList.of(1, 0, 0)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(9, ImmutableList.of(0, 0, 6)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(11, ImmutableList.of(0, 0, 6)));
    Iterator<Integer> it = queueBasedMultiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(6, 5, 1, 7, 11);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }


  @Test
  public void multiColumnSorWithNull()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    QueueBasedMultiColumnSorter queueBasedMultiColumnSorter = new QueueBasedMultiColumnSorter(4, getMultiColumnSorterElementComparator(orderByDirection));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(2, ImmutableList.of(0, 0, 2)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(3, ImmutableList.of(0, 0, 3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(4, ImmutableList.of(0, 0, 4)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(5, ImmutableList.of(0, 3, 5)));
    List list = new ArrayList();
    list.add(null);
    list.add(6);
    list.add(7);
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(6, list));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(7, ImmutableList.of(0, 0, 8)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 9)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(100, ImmutableList.of(1, 0, 0)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 3)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(9, ImmutableList.of(0, 0, 6)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(11, ImmutableList.of(0, 0, 6)));
    Iterator<Integer> it = queueBasedMultiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(6, 5, 1, 7);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }


  @Test
  public void multiColumnSortCalssCastException()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    QueueBasedMultiColumnSorter queueBasedMultiColumnSorter = new QueueBasedMultiColumnSorter(5, getMultiColumnSorterElementComparator(orderByDirection));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(1, ImmutableList.of(0, 0, 1)));
    queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(2, ImmutableList.of(0, 0, 2)));
    ISE ise = null;
    try {
      queueBasedMultiColumnSorter.add(new MultiColumnSorter.MultiColumnSorterElement(3, ImmutableList.of(0, 0, 3L)));
    }
    catch (ISE e) {
      ise = e;
    }
    Assert.assertNotNull(ise);
    Assert.assertEquals("The sorted column cannot have different types of values.", ise.getMessage());
  }
}
