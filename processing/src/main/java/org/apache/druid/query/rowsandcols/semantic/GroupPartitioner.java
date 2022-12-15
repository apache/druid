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

import java.util.List;

/**
 * A semantic interface used to partition a data set based on a given set of dimensions.
 */
@SuppressWarnings("unused")
public interface GroupPartitioner
{
  /**
   * Computes the groupings of the underlying rows based on the columns passed in for grouping.  The grouping is
   * returned as an int[], the length of the array will be equal to the number of rows of data and the values of
   * the elements of the array will be the same when the rows are part of the same group and different when the
   * rows are part of different groups.  This is contrasted with the SortedGroupPartitioner in that, the
   * groupings returned are not necessarily contiguous.  There is also no sort-order implied by the `int` values
   * assigned to each grouping.
   *
   * @param columns the columns to group with
   * @return the groupings, rows with the same int value are in the same group.  There is no sort-order implied by the
   * int values.
   */
  int[] computeGroupings(List<String> columns);
}
