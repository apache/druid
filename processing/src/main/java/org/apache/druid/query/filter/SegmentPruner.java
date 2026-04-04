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

package org.apache.druid.query.filter;

import org.apache.druid.timeline.DataSegment;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Function;

public interface SegmentPruner
{
  /**
   * Check if a {@link DataSegment} should be included or not. If this method returns false, the segment can be skipped
   * during processing.
   */
  boolean include(DataSegment segment);

  /**
   * Filter the given {@link Iterable} of objects containing a {@link DataSegment} (obtained from the converter
   * function), to reduce the overall working set which need to be processed.
   *
   * @param input      The iterable of objects to be filtered
   * @param converter  The function to convert T to {@link DataSegment} to {@link #include(DataSegment)}
   * @param <T>        This can be any type, as long as transform function is provided to extract a {@link DataSegment}
   *
   * @return The set of pruned object, in the same order as input
   */
  default <T> Collection<T> prune(Iterable<T> input, Function<T, DataSegment> converter)
  {
    // LinkedHashSet retains order from "input".
    final Set<T> retSet = new LinkedHashSet<>();

    for (T obj : input) {
      final DataSegment segment = converter.apply(obj);
      if (segment == null) {
        continue;
      }

      if (include(segment)) {
        retSet.add(obj);
      }
    }
    return retSet;
  }

  /**
   * @return combined {@link SegmentPruner}, which if both are of the same type may be merged into a new pruner of the
   * same type that contains the information of both, or if not directly combinable, implementors should create a
   * {@link CompositeSegmentPruner}
   */
  SegmentPruner combine(SegmentPruner other);
}
