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

import org.apache.druid.query.rowsandcols.RowsAndColumns;

import java.util.ArrayList;
import java.util.List;

/**
 * A semantic interface used to partition a data set based on a given set of columns.
 * <p>
 * This specifically assumes that it is working with pre-clustered data and, as such, the groups returned
 * should be contiguous and unique (that is, all rows for a given combination of values exist in only one grouping)
 */
public interface ClusteredGroupPartitioner
{
  static ClusteredGroupPartitioner fromRAC(RowsAndColumns rac)
  {
    ClusteredGroupPartitioner retVal = rac.as(ClusteredGroupPartitioner.class);
    if (retVal == null) {
      retVal = new DefaultClusteredGroupPartitioner(rac);
    }
    return retVal;
  }

  /**
   * Computes and returns a list of contiguous boundaries for independent groups.  All rows in a specific grouping
   * should have the same values for the identified columns.  Additionally, as this is assuming it is dealing with
   * clustered data, there should only be a single entry in the return value for a given set of values of the columns.
   * <p>
   * Note that implementations are not expected to do any validation that the data is pre-clustered.  There is no
   * expectation that an implementation will identify that the same cluster existed non-contiguously.  It is up to
   * the caller to ensure that data is clustered correctly before invoking this method.
   *
   * @param columns the columns to partition on
   * @return an int[] representing the start (inclusive) and stop (exclusive) offsets of boundaries.  Boundaries are
   * contiguous, so the stop of the previous boundary is the start of the subsequent one.
   */
  int[] computeBoundaries(List<String> columns);

  /**
   * Semantically equivalent to computeBoundaries, but returns a list of RowsAndColumns objects instead of just
   * boundary positions.  This is useful as it allows the concrete implementation to return RowsAndColumns objects
   * that are aware of the internal representation of the data and thus can provide optimized implementations of
   * other semantic interfaces as the "child" RowsAndColumns are used
   *
   * @param partitionColumns the columns to partition on
   * @return a list of RowsAndColumns representing the data grouped by the partition columns.
   */
  ArrayList<RowsAndColumns> partitionOnBoundaries(List<String> partitionColumns);
}
