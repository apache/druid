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

package org.apache.druid.segment.index;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.segment.column.ColumnIndexCapabilities;

import javax.annotation.Nullable;

/**
 * Common interface for bitmap indexes for use by {@link org.apache.druid.query.filter.Filter} for cursor creation, to
 * allow fast row skipping during query processing. Ideally implementaitons of this are 'lazy', and not do any real
 * work until {@link #computeBitmapResult(BitmapResultFactory, int, int, boolean)} or
 * {@link #computeBitmapResult(BitmapResultFactory, boolean)} is called.
 */
public interface BitmapColumnIndex
{
  ColumnIndexCapabilities getIndexCapabilities();

  /**
   * Compute a bitmap result wrapped with the {@link BitmapResultFactory} representing the rows matched by this index.
   * If building a cursor, use {@link #computeBitmapResult(BitmapResultFactory, int, int, boolean)} instead.
   *
   * @param bitmapResultFactory helper to format the {@link org.apache.druid.collections.bitmap.ImmutableBitmap} in a
   *                            form ready for consumption by callers
   * @param includeUnknown      mapping for Druid native two state logic system into SQL three-state logic system. If set
   *                            to true, bitmaps returned by this method should include true bits for any rows where
   *                            the matching result is 'unknown', such as from the input being null valued.
   *                            See {@link NullHandling#useThreeValueLogic()}.
   *
   * @return bitmap result representing rows matched by this index
   */
  <T> T computeBitmapResult(
      BitmapResultFactory<T> bitmapResultFactory,
      boolean includeUnknown
  );

  /**
   * Compute a bitmap result wrapped with the {@link BitmapResultFactory} representing the rows matched by this index,
   * or null if the index cannot (or should not) be computed.
   *
   * @param bitmapResultFactory helper to format the {@link org.apache.druid.collections.bitmap.ImmutableBitmap} in a
   *                            form ready for consumption by callers
   * @param applyRowCount       upper bound on number of rows this filter would be applied to, after removing rows
   *                            short-circuited by prior bundle operations. For example, given "x AND y", if "x" is
   *                            resolved using an index, then "y" will receive the number of rows that matched
   *                            the filter "x". As another example, given "x OR y", if "x" is resolved using an
   *                            index, then "y" will receive the number of rows that did *not* match the filter "x".
   * @param totalRowCount       total number of rows to be scanned if no indexes are used
   * @param includeUnknown      mapping for Druid native two state logic system into SQL three-state logic system. If
   *                            set to true, bitmaps returned by this method should include true bits for any rows where
   *                            the matching result is 'unknown', such as from the input being null valued.
   *                            See {@link NullHandling#useThreeValueLogic()}.
   *
   * @return bitmap result representing rows matched by this index
   */
  @Nullable
  default <T> T computeBitmapResult(
      BitmapResultFactory<T> bitmapResultFactory,
      int applyRowCount,
      int totalRowCount,
      boolean includeUnknown
  )
  {
    return computeBitmapResult(bitmapResultFactory, includeUnknown);
  }
}
