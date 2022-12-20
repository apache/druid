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

package org.apache.druid.query.rowsandcols.semantic;

import com.google.common.collect.Lists;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.ArrayList;

/**
 * A NaiveSorter sorts a stream of data in-place.  In the worst case, that means it needs to buffer up all
 * RowsAndColumns received before it can return anything.  This semantic interface is setup to allow an
 * implementation of RowsAndColumns to know that it is pre-sorted and potentially return sorted data early.
 *
 * The default implementation cannot actually do this, however, so it is up to the specific concrete RowsAndColumns
 * classes to provide their own implementations that can do this.
 */
public interface NaiveSortMaker
{
  interface NaiveSorter
  {
    /**
     * Adds more data to the sort.  This method can optionally return a RowsAndColumns object.  If it does return
     * a RowsAndColumns object, any data included in the return is assumed to be in sorted-order.
     *
     * @param rac the data to include in the sort
     * @return optionally, a RowsAndColumns object of data that is known to be in sorted order, null if nothing yet.
     */
    RowsAndColumns moreData(RowsAndColumns rac);

    /**
     * Indicate that there is no more data coming.
     *
     * @return A RowsAndColumns object of sorted data that has not been returned already from {@link #moreData} calls.
     */
    RowsAndColumns complete();
  }

  /**
   * Makes the NaiveSorter that will actually do the sort.  This method uses {@code List<OrderByColumnSpec>} to avoid
   * littering the code with extra objects for the same thing.  {@code OrderByColumnSpec} is only used to identify
   * which column should be sorted and in which direction.  Specifically, it has a "dimensionComparator" field which
   * seems to indicate that it's possible to provide a specific comparator ordering, this should be completely ignored
   * by implementations of the NaiveSorter interface.
   *
   * @param ordering a specification of which columns to sort in which direction
   * @return a NaiveSorter that will sort according to the provided spec
   */
  NaiveSorter make(ArrayList<OrderByColumnSpec> ordering);

  default NaiveSorter make(OrderByColumnSpec... ordering) {
    return make(Lists.newArrayList(ordering));
  }
}
