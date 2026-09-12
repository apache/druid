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

package org.apache.druid.segment.incremental;

import org.apache.druid.segment.DimensionIndexer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Shared test helpers for aggregate projections over a clustered base table.
 */
public final class ClusteredProjectionTestUtils
{
  private ClusteredProjectionTestUtils()
  {
  }

  /**
   * Decodes the distinct grouping-column tuples stored in a projection's facts holder back to their actual values.
   * The raw indexer type mirrors IncrementalIndex's own call sites: a wildcard-typed encoded key component (here a
   * String dictionary-id array) can only be passed back to its indexer through the erased signature.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Set<List<Object>> projectionGroupingTuples(OnheapIncrementalIndex index, String projectionName)
  {
    final IncrementalIndexRowSelector projection = index.getProjection(projectionName);
    final List<IncrementalIndex.DimensionDesc> dimensions = projection.getDimensions();
    final Set<List<Object>> tuples = new HashSet<>();
    for (IncrementalIndexRow row : projection.getFacts().keySet()) {
      final List<Object> tuple = new ArrayList<>(dimensions.size());
      for (int i = 0; i < dimensions.size(); i++) {
        final DimensionIndexer indexer = dimensions.get(i).getIndexer();
        tuple.add(indexer.convertUnsortedEncodedKeyComponentToActualList(row.getDims()[i]));
      }
      tuples.add(tuple);
    }
    return tuples;
  }
}
