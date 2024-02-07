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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.filter.vector.VectorValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

  public BundleInfo getInfo()
  {
    return new BundleInfo(
        index == null ? null : index.getIndexMetrics(),
        matcherBundle == null ? null : matcherBundle.getMatcherMetrics()
    );
  }

  public interface IndexBundle
  {
    List<IndexBundleInfo> getIndexMetrics();
    ImmutableBitmap getBitmap();
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
    List<MatcherBundleInfo> getMatcherMetrics();
    ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending);
    VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory);
  }

  public static class BundleInfo
  {
    private final List<IndexBundleInfo> indexes;
    private final List<MatcherBundleInfo> matchers;

    @JsonCreator
    public BundleInfo(
        @JsonProperty("indexes") List<IndexBundleInfo> indexes,
        @JsonProperty("matchers") List<MatcherBundleInfo> matchers
    )
    {
      this.indexes = indexes;
      this.matchers = matchers;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<IndexBundleInfo> getIndexes()
    {
      return indexes;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<MatcherBundleInfo> getMatchers()
    {
      return matchers;
    }

    @Override
    public String toString()
    {
      return "{indexes=" + indexes + ", matcher=" + matchers + '}';
    }
  }

  public static class IndexBundleInfo
  {
    private final String filter;
    private final int selectionSize;
    private final long buildTimeNs;

    @JsonCreator
    public IndexBundleInfo(
        @JsonProperty("filter") String filter,
        @JsonProperty("selectionSize") int selectionSize,
        @JsonProperty("buildTimeNs") long buildTimeNs
    )
    {
      this.filter = filter;
      this.selectionSize = selectionSize;
      this.buildTimeNs = buildTimeNs;
    }

    @JsonProperty
    public String getFilter()
    {
      return filter;
    }

    @JsonProperty
    public int getSelectionSize()
    {
      return selectionSize;
    }

    @JsonProperty
    public long getBuildTimeNs()
    {
      return buildTimeNs;
    }

    @Override
    public String toString()
    {
      return "{" +
             "filter=\"" + filter + '\"' +
             ", selectionSize=" + selectionSize +
             ", buildTime=" + TimeUnit.NANOSECONDS.toMicros(buildTimeNs) + "μs" +
             '}';
    }
  }

  public static class MatcherBundleInfo
  {
    private final String filter;
    @Nullable
    private final IndexBundleInfo partialIndex;

    @JsonCreator
    public MatcherBundleInfo(
        @JsonProperty("filter") String filter,
        @JsonProperty("partialIndex") @Nullable IndexBundleInfo partialIndex
    )
    {
      this.filter = filter;
      this.partialIndex = partialIndex;
    }

    @JsonProperty
    public String getFilter()
    {
      return filter;
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public IndexBundleInfo getPartialIndex()
    {
      return partialIndex;
    }

    @Override
    public String toString()
    {
      return "{" +
             "filter=\"" + filter + '\"' +
             (partialIndex != null ? ", partialIndex=" + partialIndex : "") +
             '}';
    }
  }

  public static class SimpleIndexBundle implements IndexBundle
  {
    private final List<IndexBundleInfo> indexBundles;

    private final ImmutableBitmap index;

    public SimpleIndexBundle(List<IndexBundleInfo> indexBundles, ImmutableBitmap index)
    {
      this.indexBundles = Preconditions.checkNotNull(indexBundles);
      this.index = Preconditions.checkNotNull(index);
    }

    @Override
    public List<IndexBundleInfo> getIndexMetrics()
    {
      return indexBundles;
    }

    @Override
    public ImmutableBitmap getBitmap()
    {
      return index;
    }

  }

  public static class SimpleMatcherBundle implements MatcherBundle
  {
    private final List<MatcherBundleInfo> matcherBundles;
    private final Function<ColumnSelectorFactory, ValueMatcher> matcherFn;
    private final Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn;

    public SimpleMatcherBundle(
        List<MatcherBundleInfo> matcherBundles,
        Function<ColumnSelectorFactory, ValueMatcher> matcherFn,
        Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn
    )
    {
      this.matcherBundles = Preconditions.checkNotNull(matcherBundles);
      this.matcherFn = Preconditions.checkNotNull(matcherFn);
      this.vectorMatcherFn = Preconditions.checkNotNull(vectorMatcherFn);
    }

    @Override
    public List<MatcherBundleInfo> getMatcherMetrics()
    {
      return matcherBundles;
    }

    @Override
    public ValueMatcher valueMatcher(
        ColumnSelectorFactory selectorFactory,
        Offset baseOffset,
        boolean descending
    )
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
