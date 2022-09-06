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
import org.apache.druid.query.scan.ScanQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MultiColumnSorterTests
{
  @Test
  public void singleColumnAscSort()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Integer> o1,
          MultiColumnSorter.MultiColumnSorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            } else {
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter multiColumnSorter = new MultiColumnSorter(5, comparator);
    multiColumnSorter.add(1, ImmutableList.of(1));
    multiColumnSorter.add(2, ImmutableList.of(2));
    multiColumnSorter.add(3, ImmutableList.of(3));
    multiColumnSorter.add(4, ImmutableList.of(4));
    multiColumnSorter.add(5, ImmutableList.of(5));
    multiColumnSorter.add(6, ImmutableList.of(7));
    multiColumnSorter.add(7, ImmutableList.of(8));
    multiColumnSorter.add(1, ImmutableList.of(9));
    multiColumnSorter.add(100, ImmutableList.of(0));
    multiColumnSorter.add(1, ImmutableList.of(1));
    multiColumnSorter.add(1, ImmutableList.of(3));
    multiColumnSorter.add(9, ImmutableList.of(6));
    multiColumnSorter.add(11, ImmutableList.of(6));
    Iterator<Integer> it = multiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(100, 1, 1, 2, 1);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }

  @Test
  public void singleColumnAscSortSkipNull()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Integer> o1,
          MultiColumnSorter.MultiColumnSorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            } else {
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter multiColumnSorter = new MultiColumnSorter(5, comparator);
    multiColumnSorter.add(1, ImmutableList.of(1));
    multiColumnSorter.add(2, ImmutableList.of(2));
    multiColumnSorter.add(3, ImmutableList.of(3));
    multiColumnSorter.add(4, ImmutableList.of(4));
    multiColumnSorter.add(5, ImmutableList.of(5));
    multiColumnSorter.add(6, ImmutableList.of(7));
    multiColumnSorter.add(7, ImmutableList.of(8));
    multiColumnSorter.add(1, ImmutableList.of(9));
    List list = new ArrayList();
    list.add(null);
    multiColumnSorter.add(100, list);
    multiColumnSorter.add(1, ImmutableList.of(1));
    multiColumnSorter.add(1, ImmutableList.of(3));
    multiColumnSorter.add(9, ImmutableList.of(6));
    multiColumnSorter.add(11, ImmutableList.of(6));
    Iterator<Integer> it = multiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(1, 1, 2, 1, 3);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }

  @Test
  public void multiColumnSort()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Integer> o1,
          MultiColumnSorter.MultiColumnSorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            } else {
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter multiColumnSorter = new MultiColumnSorter(5, comparator);
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 1));
    multiColumnSorter.add(2, ImmutableList.of(0, 0, 2));
    multiColumnSorter.add(3, ImmutableList.of(0, 0, 3));
    multiColumnSorter.add(4, ImmutableList.of(0, 0, 4));
    multiColumnSorter.add(5, ImmutableList.of(0, 3, 5));
    multiColumnSorter.add(6, ImmutableList.of(0, 6, 7));
    multiColumnSorter.add(7, ImmutableList.of(0, 0, 8));
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 9));
    multiColumnSorter.add(100, ImmutableList.of(1, 0, 0));
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 1));
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 3));
    multiColumnSorter.add(9, ImmutableList.of(0, 0, 6));
    multiColumnSorter.add(11, ImmutableList.of(0, 0, 6));
    Iterator<Integer> it = multiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(6, 5, 1, 7, 11);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }


  @Test
  public void multiColumnSorSkipNull()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Integer> o1,
          MultiColumnSorter.MultiColumnSorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            } else {
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter multiColumnSorter = new MultiColumnSorter(4, comparator);
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 1));
    multiColumnSorter.add(2, ImmutableList.of(0, 0, 2));
    multiColumnSorter.add(3, ImmutableList.of(0, 0, 3));
    multiColumnSorter.add(4, ImmutableList.of(0, 0, 4));
    multiColumnSorter.add(5, ImmutableList.of(0, 3, 5));
    List list = new ArrayList();
    list.add(null);
    list.add(6);
    list.add(7);
    multiColumnSorter.add(6, list);
    multiColumnSorter.add(7, ImmutableList.of(0, 0, 8));
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 9));
    multiColumnSorter.add(100, ImmutableList.of(1, 0, 0));
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 1));
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 3));
    multiColumnSorter.add(9, ImmutableList.of(0, 0, 6));
    multiColumnSorter.add(11, ImmutableList.of(0, 0, 6));
    Iterator<Integer> it = multiColumnSorter.drain();
    List<Integer> expectedValues = ImmutableList.of(5, 1, 7, 9);
    int i = 0;
    while (it.hasNext()) {
      Assert.assertEquals(expectedValues.get(i++), it.next());
    }
  }


  @Test
  public void multiColumnSortCalssCastException()
  {
    List<String> orderByDirection = ImmutableList.of("ASCENDING", "DESCENDING", "DESCENDING");
    Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>> comparator = new Comparator<MultiColumnSorter.MultiColumnSorterElement<Integer>>()
    {
      @Override
      public int compare(
          MultiColumnSorter.MultiColumnSorterElement<Integer> o1,
          MultiColumnSorter.MultiColumnSorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          if (!o1.getOrderByColumValues().get(i).equals(o2.getOrderByColumValues().get(i))) {
            if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
              return o1.getOrderByColumValues().get(i).compareTo(o2.getOrderByColumValues().get(i));
            } else {
              return o2.getOrderByColumValues().get(i).compareTo(o1.getOrderByColumValues().get(i));
            }
          }
        }
        return 0;
      }
    };
    MultiColumnSorter multiColumnSorter = new MultiColumnSorter(5, comparator);
    multiColumnSorter.add(1, ImmutableList.of(0, 0, 1));
    multiColumnSorter.add(2, ImmutableList.of(0, 0, 2));
    ISE ise = null;
    try {
      multiColumnSorter.add(3, ImmutableList.of(0, 0, 3L));
    }
    catch (ISE e) {
      ise = e;
    }
    Assert.assertEquals("Multiple values of different types scanOrderBy are not allowed in the same column.", ise.getMessage());
  }
}
