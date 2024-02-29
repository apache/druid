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
import org.apache.druid.segment.column.ColumnIndexCapabilities;
import org.apache.druid.segment.column.SimpleColumnIndexCapabilities;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * FilterBundle is a container for all the goodies used for producing filtered cursors, a {@link ImmutableBitmap} if
 * the filter can use an index, and/or a {@link MatcherBundle} which contains functions to build {@link ValueMatcher}
 * and {@link VectorValueMatcher} for any filters which must be evaluated row by row during the cursor scan. Cursors
 * will use everything that is non-null, and at least one of index or matcher bundle MUST be set. If both index and
 * matcher is present, the cursor will effectively perform a logical AND operation; i.e. if the index matches a row
 * then the value matcher must also match the row for the cursor to provide it to the selectors built on top of it.
 * <p>
 * There are a few cases where the filter should set both indexes and matchers. For example, if the filter is a
 * composite filter which can be partitioned, such as {@link org.apache.druid.segment.filter.AndFilter}, then the filter
 * can be partitioned due to the intersection nature of AND, so the index can be set to reduce the number of rows and
 * the matcher bundle will build a matcher which will filter the remainder. This strategy of having both an index and a
 * matcher can also can model the case where the index is an inexact match and so a matcher must be used to ensure that
 * the remaining values actually match the filter.
 */
public class FilterBundle
{
  public static FilterBundle allFalse(long constructionTime, ImmutableBitmap emptyBitmap)
  {
    return new FilterBundle(
        new FilterBundle.SimpleIndexBundle(
            new FilterBundle.IndexBundleInfo(() -> FalseFilter.instance().toString(), 0, constructionTime, null),
            emptyBitmap,
            SimpleColumnIndexCapabilities.getConstant()
        ),
        null
    );
  }

  @Nullable
  private final IndexBundle indexBundle;
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
    this.indexBundle = index;
    this.matcherBundle = matcherBundle;
  }


  @Nullable
  public IndexBundle getIndex()
  {
    return indexBundle;
  }

  @Nullable
  public MatcherBundle getMatcherBundle()
  {
    return matcherBundle;
  }

  public BundleInfo getInfo()
  {
    return new BundleInfo(
        indexBundle == null ? null : indexBundle.getIndexInfo(),
        matcherBundle == null ? null : matcherBundle.getMatcherInfo()
    );
  }

  public boolean hasIndex()
  {
    return indexBundle != null;
  }

  public boolean hasMatcher()
  {
    return matcherBundle != null;
  }

  public interface IndexBundle
  {
    IndexBundleInfo getIndexInfo();
    ImmutableBitmap getBitmap();
    ColumnIndexCapabilities getIndexCapabilities();
  }

  /**
   * Builder of {@link ValueMatcher} and {@link VectorValueMatcher}. The
   * {@link #valueMatcher(ColumnSelectorFactory, Offset, boolean)} function also passes in the base offset and whether
   * the offset is 'descending' or not, to allow filters more flexibility in value matcher creation.
   * {@link org.apache.druid.segment.filter.OrFilter} uses these extra parameters to allow partial use of indexes to
   * create a synthetic value matcher that checks if the row is set in the bitmap, instead of purely using value
   * matchers, with {@link org.apache.druid.segment.filter.OrFilter#convertIndexToValueMatcher}.
   */
  public interface MatcherBundle
  {
    MatcherBundleInfo getMatcherInfo();
    ValueMatcher valueMatcher(ColumnSelectorFactory selectorFactory, Offset baseOffset, boolean descending);
    VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory, ReadableVectorOffset baseOffset);
  }

  public static class SimpleIndexBundle implements IndexBundle
  {
    private final IndexBundleInfo info;
    private final ImmutableBitmap index;
    private final ColumnIndexCapabilities indexCapabilities;

    public SimpleIndexBundle(IndexBundleInfo info, ImmutableBitmap index, ColumnIndexCapabilities indexCapabilities)
    {
      this.info = Preconditions.checkNotNull(info);
      this.index = Preconditions.checkNotNull(index);
      this.indexCapabilities = Preconditions.checkNotNull(indexCapabilities);
    }

    @Override
    public IndexBundleInfo getIndexInfo()
    {
      return info;
    }

    @Override
    public ImmutableBitmap getBitmap()
    {
      return index;
    }

    @Override
    public ColumnIndexCapabilities getIndexCapabilities()
    {
      return indexCapabilities;
    }
  }

  public static class SimpleMatcherBundle implements MatcherBundle
  {
    private final MatcherBundleInfo matcherInfo;
    private final Function<ColumnSelectorFactory, ValueMatcher> matcherFn;
    private final Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn;

    public SimpleMatcherBundle(
        MatcherBundleInfo matcherInfo,
        Function<ColumnSelectorFactory, ValueMatcher> matcherFn,
        Function<VectorColumnSelectorFactory, VectorValueMatcher> vectorMatcherFn
    )
    {
      this.matcherInfo = Preconditions.checkNotNull(matcherInfo);
      this.matcherFn = Preconditions.checkNotNull(matcherFn);
      this.vectorMatcherFn = Preconditions.checkNotNull(vectorMatcherFn);
    }

    @Override
    public MatcherBundleInfo getMatcherInfo()
    {
      return matcherInfo;
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
    public VectorValueMatcher vectorMatcher(VectorColumnSelectorFactory selectorFactory, ReadableVectorOffset baseOffset)
    {
      return vectorMatcherFn.apply(selectorFactory);
    }
  }

  public static class BundleInfo
  {
    private final IndexBundleInfo index;
    private final MatcherBundleInfo matcher;

    @JsonCreator
    public BundleInfo(
        @JsonProperty("index") @Nullable IndexBundleInfo index,
        @JsonProperty("matcher") @Nullable MatcherBundleInfo matcher
    )
    {
      this.index = index;
      this.matcher = matcher;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public IndexBundleInfo getIndex()
    {
      return index;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public MatcherBundleInfo getMatcher()
    {
      return matcher;
    }

    @Override
    public String toString()
    {
      return "{index=" + index + ", matcher=" + matcher + '}';
    }
  }

  public static class IndexBundleInfo
  {
    private final Supplier<String> filter;
    private final List<IndexBundleInfo> indexes;
    private final int selectionSize;
    private final long buildTimeNs;

    public IndexBundleInfo(
        Supplier<String> filterString,
        int selectionSize,
        long buildTimeNs,
        @Nullable List<IndexBundleInfo> indexes
    )
    {
      this.filter = filterString;
      this.selectionSize = selectionSize;
      this.buildTimeNs = buildTimeNs;
      this.indexes = indexes;
    }

    @JsonProperty
    public String getFilter()
    {
      return filter.get();
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

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<IndexBundleInfo> getIndexes()
    {
      return indexes;
    }

    @Override
    public String toString()
    {
      return "{" +
             "filter=\"" + filter.get() + '\"' +
             ", selectionSize=" + selectionSize +
             ", buildTime=" + TimeUnit.NANOSECONDS.toMicros(buildTimeNs) + "μs" +
             (indexes != null ? ", indexes=" + indexes : "") +
             '}';
    }
  }

  public static class MatcherBundleInfo
  {
    private final Supplier<String> filter;
    @Nullable
    final List<MatcherBundleInfo> matchers;

    @Nullable
    private final IndexBundleInfo partialIndex;

    public MatcherBundleInfo(
        Supplier<String> filter,
        @Nullable IndexBundleInfo partialIndex,
        @Nullable List<MatcherBundleInfo> matchers
    )
    {
      this.filter = filter;
      this.matchers = matchers;
      this.partialIndex = partialIndex;
    }

    @JsonProperty
    public String getFilter()
    {
      return filter.get();
    }

    @Nullable
    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public IndexBundleInfo getPartialIndex()
    {
      return partialIndex;
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
      return "{" +
             "filter=\"" + filter.get() + '\"' +
             (partialIndex != null ? ", partialIndex=" + partialIndex : "") +
             (matchers != null ? ", matchers=" + matchers : "") +
             '}';
    }
  }
}
