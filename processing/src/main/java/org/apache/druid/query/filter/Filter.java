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

import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

@SubclassesMustOverrideEqualsAndHashCode
public interface Filter
{
  /**
   * Compute indexes and build a container {@link FilterBundle} to be used during
   * {@link org.apache.druid.segment.Cursor} or {@link org.apache.druid.segment.vector.VectorCursor} creation, combining
   * the computed outputs of {@link #getBitmapColumnIndex(ColumnIndexSelector)} as well as references to
   * {@link #makeMatcher(ColumnSelectorFactory)} and {@link #makeVectorMatcher(VectorColumnSelectorFactory)}.
   * <p>
   * Filters populating the {@link FilterBundle} container should only set the values which MUST be evaluated by the
   * cursor. If both are set, the cursor will effectively perform a logical AND to combine them.
   * See {@link FilterBundle} for additional details.
   *
   * @param columnIndexSelector - provides {@link org.apache.druid.segment.column.ColumnIndexSupplier} to fetch column
   *                              indexes and {@link org.apache.druid.collections.bitmap.BitmapFactory} to manipulate
   *                              them
   * @param bitmapResultFactory - wrapper for {@link ImmutableBitmap} operations to tie into
   *                              {@link org.apache.druid.query.QueryMetrics} and build the output indexes
   * @param selectionRowCount   - number of rows selected so far by any previous bundle computations
   * @param totalRowCount       - total number of rows to be scanned if no indexes are applied
   * @param includeUnknown      - mapping for Druid native two state logic system into SQL three-state logic system. If
   *                              set to true, bitmaps returned by this method should include true bits for any rows
   *                              where the matching result is 'unknown', such as from the input being null valued.
   *                              See {@link NullHandling#useThreeValueLogic()}
   * @return                    - {@link FilterBundle} containing any indexes and/or matchers that are needed to build
   *                              a cursor
   * @param <T>                 - Type of {@link BitmapResultFactory} results, {@link ImmutableBitmap} by default
   */
  default <T> FilterBundle makeFilterBundle(
      ColumnIndexSelector columnIndexSelector,
      BitmapResultFactory<T> bitmapResultFactory,
      int selectionRowCount,
      int totalRowCount,
      boolean includeUnknown
  )
  {
    final FilterBundle.IndexBundle indexBundle;
    final boolean needMatcher;
    final BitmapColumnIndex columnIndex = getBitmapColumnIndex(columnIndexSelector);
    if (columnIndex != null) {
      final long bitmapConstructionStartNs = System.nanoTime();
      final T result = columnIndex.computeBitmapResult(
          bitmapResultFactory,
          selectionRowCount,
          totalRowCount,
          includeUnknown
      );
      final long totalConstructionTimeNs = System.nanoTime() - bitmapConstructionStartNs;
      if (result == null) {
        indexBundle = null;
      } else {
        final ImmutableBitmap bitmap = bitmapResultFactory.toImmutableBitmap(result);
        indexBundle = new FilterBundle.SimpleIndexBundle(
            new FilterBundle.IndexBundleInfo(this::toString, bitmap.size(), totalConstructionTimeNs, null),
            bitmap,
            columnIndex.getIndexCapabilities()
        );
      }
      needMatcher = result == null || !columnIndex.getIndexCapabilities().isExact();
    } else {
      indexBundle = null;
      needMatcher = true;
    }
    final FilterBundle.SimpleMatcherBundle matcherBundle;
    if (needMatcher) {
      matcherBundle = new FilterBundle.SimpleMatcherBundle(
          new FilterBundle.MatcherBundleInfo(this::toString, null, null),
          this::makeMatcher,
          this::makeVectorMatcher
      );
    } else {
      matcherBundle = null;
    }
    return new FilterBundle(indexBundle, matcherBundle);
  }

  /**
   * Returns a {@link BitmapColumnIndex} if this filter supports using a bitmap index for filtering for the given input
   * {@link ColumnIndexSelector}. The {@link BitmapColumnIndex} can be used to compute into a bitmap indicating rows
   * that match this filter result {@link BitmapColumnIndex#computeBitmapResult(BitmapResultFactory, boolean)}, or
   * examine details about the index prior to computing it, via {@link BitmapColumnIndex#getIndexCapabilities()}.
   *
   * @param selector Object used to create BitmapColumnIndex
   *
   * @return BitmapColumnIndex that can build ImmutableBitmap of matched row numbers
   */
  @Nullable
  BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector);

  /**
   * Get a {@link ValueMatcher} that applies this filter to row values.
   *
   * @param factory Object used to create ValueMatchers
   *
   * @return ValueMatcher that applies this filter to row values.
   */
  ValueMatcher makeMatcher(ColumnSelectorFactory factory);

  /**
   * Get a {@link VectorValueMatcher} that applies this filter to row vectors.
   *
   * @param factory Object used to create ValueMatchers
   *
   * @return VectorValueMatcher that applies this filter to row vectors.
   */
  default VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    throw new UOE("Filter[%s] cannot vectorize", getClass().getName());
  }

  /**
   * Returns true if this filter can produce a vectorized matcher from its "makeVectorMatcher" method.
   * @param inspector Supplies type information for the selectors this filter will match against
   */
  default boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return false;
  }

  /**
   * Set of columns used by a filter.
   */
  Set<String> getRequiredColumns();

  /**
   * Returns true is this filter is able to return a copy of this filter that is identical to this filter except that it
   * operates on different columns, based on a renaming map.
   */
  default boolean supportsRequiredColumnRewrite()
  {
    return false;
  }

  /**
   * Return a copy of this filter that is identical to the this filter except that it operates on different columns,
   * based on a renaming map where the key is the column to be renamed in the filter, and the value is the new
   * column name.
   *
   * For example, if I have a filter (A = hello), and I have a renaming map (A -> B),
   * this should return the filter (B = hello)
   *
   * @param columnRewrites Column rewrite map
   * @return Copy of this filter that operates on new columns based on the rewrite map
   */
  default Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    throw new UnsupportedOperationException("Required column rewrite is not supported by this filter.");
  }
}
