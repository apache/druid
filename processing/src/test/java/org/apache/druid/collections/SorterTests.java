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

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SorterTests
{
  public void addData(Sorter<Integer> sorter, Integer data, Integer... sortData)
  {
    sorter.add(new Sorter.SorterElement(data, ImmutableList.of(sortData)));
  }

  @Nonnull
  protected Comparator<Sorter.SorterElement<Integer>> getMultiColumnSorterElementComparator(List<String> orderByDirection)
  {
    Comparator<Sorter.SorterElement<Integer>> comparator = new Comparator<Sorter.SorterElement<Integer>>()
    {
      @Override
      public int compare(
          Sorter.SorterElement<Integer> o1,
          Sorter.SorterElement<Integer> o2
      )
      {
        for (int i = 0; i < o1.getOrderByColumValues().size(); i++) {
          int compare;
          if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
            compare = Comparators.<Comparable>naturalNullsFirst()
                                 .compare(o1.getOrderByColumValues().get(i), o2.getOrderByColumValues().get(i));
          } else {
            compare = Comparators.<Comparable>naturalNullsFirst()
                                 .compare(o2.getOrderByColumValues().get(i), o1.getOrderByColumValues().get(i));
          }
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
    return comparator;
  }

  protected void singleColumnAscSortDatas(Sorter<Integer> sorter, List<Integer> expectedValues)
  {

    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(1)));
    sorter.add(new Sorter.SorterElement(2, ImmutableList.of(2)));
    sorter.add(new Sorter.SorterElement(3, ImmutableList.of(3)));
    sorter.add(new Sorter.SorterElement(4, ImmutableList.of(4)));
    sorter.add(new Sorter.SorterElement(5, ImmutableList.of(5)));
    sorter.add(new Sorter.SorterElement(6, ImmutableList.of(7)));
    sorter.add(new Sorter.SorterElement(7, ImmutableList.of(8)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(9)));
    sorter.add(new Sorter.SorterElement(100, ImmutableList.of(0)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(1)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(3)));
    sorter.add(new Sorter.SorterElement(9, ImmutableList.of(6)));
    sorter.add(new Sorter.SorterElement(11, ImmutableList.of(6)));

    expectedValues.addAll(ImmutableList.of(100, 1, 2, 3, 4));
  }

  protected void singleColumnAscSortNaturalNullsFirstDatas(Sorter<Integer> sorter, List<Integer> expectedValues)
  {

    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(1)));
    sorter.add(new Sorter.SorterElement(2, ImmutableList.of(2)));
    sorter.add(new Sorter.SorterElement(3, ImmutableList.of(3)));
    sorter.add(new Sorter.SorterElement(4, ImmutableList.of(4)));
    sorter.add(new Sorter.SorterElement(5, ImmutableList.of(5)));
    sorter.add(new Sorter.SorterElement(6, ImmutableList.of(7)));
    sorter.add(new Sorter.SorterElement(7, ImmutableList.of(8)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(9)));
    List list = new ArrayList();
    list.add(null);
    sorter.add(new Sorter.SorterElement(100, list));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(1)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(3)));
    sorter.add(new Sorter.SorterElement(9, ImmutableList.of(6)));
    sorter.add(new Sorter.SorterElement(11, ImmutableList.of(6)));

    expectedValues.addAll(ImmutableList.of(100, 1, 2, 3, 4));
  }

  protected void multiColumnSortDatas(Sorter<Integer> sorter, List<Integer> expectedValues)
  {
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 1)));
    sorter.add(new Sorter.SorterElement(2, ImmutableList.of(0, 0, 2)));
    sorter.add(new Sorter.SorterElement(3, ImmutableList.of(0, 0, 3)));
    sorter.add(new Sorter.SorterElement(4, ImmutableList.of(0, 0, 4)));
    sorter.add(new Sorter.SorterElement(5, ImmutableList.of(0, 3, 5)));
    sorter.add(new Sorter.SorterElement(6, ImmutableList.of(0, 6, 7)));
    sorter.add(new Sorter.SorterElement(7, ImmutableList.of(0, 0, 8)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 9)));
    sorter.add(new Sorter.SorterElement(100, ImmutableList.of(1, 0, 0)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 1)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 3)));
    sorter.add(new Sorter.SorterElement(9, ImmutableList.of(0, 0, 6)));
    sorter.add(new Sorter.SorterElement(11, ImmutableList.of(0, 0, 6)));

    expectedValues.addAll(ImmutableList.of(6, 5, 1, 7, 9));
  }


  protected void multiColumnSorWithNullDatas(Sorter<Integer> sorter, List<Integer> expectedValues)
  {
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 1)));
    sorter.add(new Sorter.SorterElement(2, ImmutableList.of(0, 0, 2)));
    sorter.add(new Sorter.SorterElement(3, ImmutableList.of(0, 0, 3)));
    sorter.add(new Sorter.SorterElement(4, ImmutableList.of(0, 0, 4)));
    sorter.add(new Sorter.SorterElement(5, ImmutableList.of(0, 3, 5)));
    List list = new ArrayList();
    list.add(null);
    list.add(6);
    list.add(7);
    sorter.add(new Sorter.SorterElement(6, list));
    sorter.add(new Sorter.SorterElement(7, ImmutableList.of(0, 0, 8)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 9)));
    sorter.add(new Sorter.SorterElement(100, ImmutableList.of(1, 0, 0)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 1)));
    sorter.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 3)));
    sorter.add(new Sorter.SorterElement(9, ImmutableList.of(0, 0, 6)));
    sorter.add(new Sorter.SorterElement(11, ImmutableList.of(0, 0, 6)));

    expectedValues.addAll(ImmutableList.of(6, 5, 1, 7));
  }

  protected void multiColumnSortCalssCastExceptionDatas(Sorter sort)
  {
    sort.add(new Sorter.SorterElement(1, ImmutableList.of(0, 0, 1)));
    sort.add(new Sorter.SorterElement(2, ImmutableList.of(0, 0, 2)));
    ISE ise = null;
    try {
      sort.add(new Sorter.SorterElement(3, ImmutableList.of(0, 0, 3L)));
      sort.drainElement();
    }
    catch (ISE e) {
      throw e;
    }
  }

}
