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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SorterTests
{

  public void addData(Sorter<Integer> sorter, List<Integer> datas)
  {
    sorter.add(datas);
  }

  @Nonnull
  protected Comparator<List<Integer>> getMultiColumnSorterElementComparator(
      List<String> orderByDirection,
      List<Integer> orderByIdxs
  )
  {
    Comparator<List<Integer>> comparator = (o1, o2) -> {
      for (int i = 0; i < orderByIdxs.size(); i++) {
        int compare;
        if (ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))) {
          compare = Comparators.<Comparable>naturalNullsFirst()
                               .compare(o1.get(orderByIdxs.get(i)), o2.get(orderByIdxs.get(i)));
        } else {
          compare = Comparators.<Comparable>naturalNullsFirst()
                               .compare(o2.get(orderByIdxs.get(i)), o1.get(orderByIdxs.get(i)));
        }
        if (compare != 0) {
          return compare;
        }
      }
      return 0;
    };
    return comparator;
  }

  private List<Object> getDatas(Object... datas)
  {
    return Arrays.asList(datas);
  }

  protected void singleColumnAscSortDatas(Sorter<Object> sorter, List<Integer> expectedValues)
  {
    sorter.add(getDatas(1, 1));
    sorter.add(getDatas(2, 2));
    sorter.add(getDatas(3, 3));
    sorter.add(getDatas(4, 4));
    sorter.add(getDatas(5, 5));
    sorter.add(getDatas(6, 7));
    sorter.add(getDatas(7, 8));
    sorter.add(getDatas(1, 9));
    sorter.add(getDatas(100, 0));
    sorter.add(getDatas(1, 1));
    sorter.add(getDatas(1, 3));
    sorter.add(getDatas(9, 6));
    sorter.add(getDatas(11, 6));

    expectedValues.addAll(ImmutableList.of(100, 1, 2, 3, 4));
  }

  protected void singleColumnAscSortNaturalNullsFirstDatas(Sorter<Object> sorter, List<Integer> expectedValues)
  {

    sorter.add(getDatas(1, 1));
    sorter.add(getDatas(2, 2));
    sorter.add(getDatas(3, 3));
    sorter.add(getDatas(4, 4));
    sorter.add(getDatas(5, 5));
    sorter.add(getDatas(6, 7));
    sorter.add(getDatas(7, 8));
    sorter.add(getDatas(1, 9));
    sorter.add(getDatas(100, null));
    sorter.add(getDatas(1, 1));
    sorter.add(getDatas(1, 3));
    sorter.add(getDatas(9, 6));
    sorter.add(getDatas(11, 6));

    expectedValues.addAll(ImmutableList.of(100, 1, 2, 3, 4));
  }

  protected void multiColumnSortDatas(Sorter<Object> sorter, List<Integer> expectedValues)
  {
    sorter.add(getDatas(1, 0, 0, 1));
    sorter.add(getDatas(2, 0, 0, 2));
    sorter.add(getDatas(3, 0, 0, 3));
    sorter.add(getDatas(4, 0, 0, 4));
    sorter.add(getDatas(5, 0, 3, 5));
    sorter.add(getDatas(6, 0, 6, 7));
    sorter.add(getDatas(7, 0, 0, 8));
    sorter.add(getDatas(1, 0, 0, 9));
    sorter.add(getDatas(100, 1, 0, 0));
    sorter.add(getDatas(1, 0, 0, 1));
    sorter.add(getDatas(1, 0, 0, 3));
    sorter.add(getDatas(9, 0, 0, 6));
    sorter.add(getDatas(11, 0, 0, 6));

    expectedValues.addAll(ImmutableList.of(6, 5, 1, 7, 9));
  }


  protected void multiColumnSorWithNullDatas(Sorter<Object> sorter, List<Integer> expectedValues)
  {
    sorter.add(getDatas(1, 0, 0, 1));
    sorter.add(getDatas(2, 0, 0, 2));
    sorter.add(getDatas(3, 0, 0, 3));
    sorter.add(getDatas(4, 0, 0, 4));
    sorter.add(getDatas(5, 0, 3, 5));
    sorter.add(getDatas(6, null, 6, 7));
    sorter.add(getDatas(7, 0, 0, 8));
    sorter.add(getDatas(1, 0, 0, 9));
    sorter.add(getDatas(100, 1, 0, 0));
    sorter.add(getDatas(1, 0, 0, 1));
    sorter.add(getDatas(1, 0, 0, 3));
    sorter.add(getDatas(9, 0, 0, 6));
    sorter.add(getDatas(11, 0, 0, 6));

    expectedValues.addAll(ImmutableList.of(6, 5, 1, 7));
  }

  protected void multiColumnSortCalssCastExceptionDatas(Sorter sorter)
  {
    sorter.add(getDatas(1, 0, 0, 1));
    sorter.add(getDatas(2, 0, 0, 2));
    ISE ise = null;
    try {
      sorter.add(getDatas(3, 0, 0, 3L));
      sorter.drainElement();
    }
    catch (ISE e) {
      throw e;
    }
  }

}
