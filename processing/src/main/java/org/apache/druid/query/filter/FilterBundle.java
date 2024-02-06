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
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * Itsa me, your filter bundle. This is a container for all the goodies used for producing filtered cursors,
 * a {@link ImmutableBitmap} if the filter can use an index, and a {@link MatcherBundle} which contains functions to
 * build {@link ValueMatcher} and {@link VectorValueMatcher} for any filters which must be evaluated row by row during
 * the cursor scan. Cursors will use everything that is non-null, and at least one of index or matcher bundle MUST be
 * set.
 * <p>
 * There are a few cases where the filter should set both indexes and matchers. For example, if the filter is a
 * composite filter which can be partitioned, such as {@link org.apache.druid.segment.filter.AndFilter}, then the filter
 * can be partitioned due to the intersection nature of AND, so the index can be set to reduce the number of rows and
 * the matcher bundle will build a matcher which will filter the remainder. This can also be set in the case the index
 * is an inexact match, and a matcher must be used to ensure that the remaining values actually match the filter.
 */
public class FilterBundle
{
  @Nullable
  private final IndexBundle index;
  @Nullable
  private final MatcherBundle matcherBundle;

  public FilterBundle(
      @Nullable IndexBundle index,
      @Nullable MatcherBundle matcherBundle
  )
  {
    Preconditions.checkArgument(
        index != null || matcherBundle != null,
        "At least one of index or matcher must be not null"
    );
    this.index = index;
    this.matcherBundle = matcherBundle;
  }


  @Nullable
  public IndexBundle getIndex()
  {
    return index;
  }

  @Nullable
  public MatcherBundle getMatcherBundle()
  {
    return matcherBundle;
  }

  @Override
  public String toString()
  {
    return "index[" + (index != null ? index.getFilterString() : null) +
           "], matcher[" + (matcherBundle != null ? matcherBundle.getFilterString() : null) +
           ']';
  }

  public interface IndexBundle
  {
    String getFilterString();
    ImmutableBitmap getBitmap();
  }

  public static class SimpleIndexBundle implements IndexBundle
  {
    private final String filterString;
    private final ImmutableBitmap index;

    public SimpleIndexBundle(String filterString, ImmutableBitmap index)
    {
      this.filterString = Preconditions.checkNotNull(filterString);
      this.index = Preconditions.checkNotNull(index);
    }

    @Override
    public String getFilterString()
    {
      return filterString;
    }

    @Override
    public ImmutableBitmap getBitmap()
    {
      return index;
    }
  }

  /**
   * Builder of {@link ValueMatcher} and {@link VectorValueMatcher}. The
   * {@link #valueMatcher(ColumnSelectorFactory, Offset, boolean)} function also passes in the base offset and whether
   * the offset is 'descending' or not, to allow filters more flexibility in value matcher creation.
   * {@link org.apache.druid.segment.filter.OrFilter} uses these extra parameters to allow partial use of indexes to
   * create a synthetic value matcher that checks if the row is set in the bitmap, instead of purely using value
   * matchers, with {@link org.apache.druid.segment.filter.OrFilter.CursorOffsetHolderRowOffsetMatcherFactory}.
   */
  public interface MatcherBundle
  {
    String getFilterString();
    ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending);
    VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory);
  }

  public static class SimpleMatcherBundle implements MatcherBundle
  {
    private final String filterString;
    private final Function<ColumnSelectorFactory, ValueMatcher> matcherFn;
    private final Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn;

    public SimpleMatcherBundle(
        String filterString,
        Function<ColumnSelectorFactory, ValueMatcher> matcherFn,
        Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn
    )
    {
      this.filterString = Preconditions.checkNotNull(filterString);
      this.matcherFn = Preconditions.checkNotNull(matcherFn);
      this.vectorMatcherFn = Preconditions.checkNotNull(vectorMatcherFn);
    }

    @Override
    public String getFilterString()
    {
      return filterString;
    }

    @Override
    public ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending)
    {
      return matcherFn.apply(selectorFactory);
    }

    @Override
    public VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory)
    {
      return vectorMatcherFn.apply(selectorFactory);
    }
  }
}
