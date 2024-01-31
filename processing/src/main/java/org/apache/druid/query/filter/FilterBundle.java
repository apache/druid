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

import com.google.common.base.Preconditions;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Itsa me, your filter bundle. This is a container for all of the goodies used for producing filtered cursors,
 * a {@link ImmutableBitmap} if the filter can use an index, and functions to build {@link ValueMatcher} and
 * {@link VectorValueMatcher} given a {@link ColumnSelectorFactory} or {@link VectorColumnSelectorFactory} respectively
 * for any filters which must be evaluated row by row during the scan. Cursors will use everything that is non-null,
 * and at least one of index or matcher creators MUST be set.
 * <p>
 * There are a few cases where the filter should set both indexes and matchers. For example, if the filter is a
 * composite filter which can be partitioned, such as {@link org.apache.druid.segment.filter.AndFilter}, then index can
 * be set to reduce the number of rows and the value matcher will filter the remainder. This can also be used if the
 * index is an in-exact match, and a matcher must be used to ensure that the remaining values actually match the filter.
 * <p>
 * {@link #partialIndex} is used to allow {@link org.apache.druid.segment.FilteredOffset} to create a
 * {@link ValueMatcher} that can partially use indexes, mostly for {@link org.apache.druid.segment.filter.OrFilter}.
 */
public class FilterBundle
{
  @Nullable
  private final ImmutableBitmap index;
  @Nullable
  private final ImmutableBitmap partialIndex;
  @Nullable
  private final Function<ColumnSelectorFactory, ValueMatcher> matcherFn;
  @Nullable
  private final Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn;

  public FilterBundle(
      @Nullable ImmutableBitmap index,
      @Nullable ImmutableBitmap partialIndex,
      @Nullable Function<ColumnSelectorFactory, ValueMatcher> matcherFn,
      @Nullable Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn
  )
  {
    Preconditions.checkArgument(
        index != null || matcherFn != null,
        "At least one of index or matcher must be not null"
    );
    this.index = index;
    this.partialIndex = partialIndex;
    this.matcherFn = matcherFn;
    this.vectorMatcherFn = vectorMatcherFn;
  }


  @Nullable
  public ImmutableBitmap getIndex()
  {
    return index;
  }

  @Nullable
  public ImmutableBitmap getPartialIndex()
  {
    return partialIndex;
  }

  @Nullable
  public ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory)
  {
    if (matcherFn == null) {
      return null;
    }
    return matcherFn.apply(selectorFactory);
  }

  @Nullable
  public VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory)
  {
    if (vectorMatcherFn == null) {
      return null;
    }
    return vectorMatcherFn.apply(selectorFactory);
  }
}
