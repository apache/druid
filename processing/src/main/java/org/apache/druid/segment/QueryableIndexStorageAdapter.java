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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 *
 */
public class QueryableIndexStorageAdapter implements StorageAdapter
{
  public static final int DEFAULT_VECTOR_SIZE = 512;

  private final QueryableIndex index;

  @Nullable
  private volatile DateTime minTime;

  @Nullable
  private volatile DateTime maxTime;

  public QueryableIndexStorageAdapter(QueryableIndex index)
  {
    this.index = index;
  }

  @Override
  public Interval getInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return index.getAvailableDimensions();
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    HashSet<String> columnNames = Sets.newHashSet(index.getColumnNames());
    return Sets.difference(columnNames, Sets.newHashSet(index.getAvailableDimensions()));
  }

  @Override
  public int getDimensionCardinality(String dimension)
  {
    ColumnHolder columnHolder = index.getColumnHolder(dimension);
    if (columnHolder == null) {
      // NullDimensionSelector has cardinality = 1 (one null, nothing else).
      return 1;
    }
    try (BaseColumn col = columnHolder.getColumn()) {
      if (!(col instanceof DictionaryEncodedColumn)) {
        return Integer.MAX_VALUE;
      }
      return ((DictionaryEncodedColumn) col).getCardinality();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }

  @Override
  public DateTime getMinTime()
  {
    if (minTime == null) {
      // May be called a few times in parallel when first populating minTime, but this is benign, so allow it.
      populateMinMaxTime();
    }

    return minTime;
  }

  @Override
  public DateTime getMaxTime()
  {
    if (maxTime == null) {
      // May be called a few times in parallel when first populating maxTime, but this is benign, so allow it.
      populateMinMaxTime();
    }

    return maxTime;
  }

  @Override
  @Nullable
  public Comparable getMinValue(String dimension)
  {
    ColumnHolder columnHolder = index.getColumnHolder(dimension);
    if (columnHolder != null && columnHolder.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = columnHolder.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(0) : null;
    }
    return null;
  }

  @Override
  @Nullable
  public Comparable getMaxValue(String dimension)
  {
    ColumnHolder columnHolder = index.getColumnHolder(dimension);
    if (columnHolder != null && columnHolder.getCapabilities().hasBitmapIndexes()) {
      BitmapIndex bitmap = columnHolder.getBitmapIndex();
      return bitmap.getCardinality() > 0 ? bitmap.getValue(bitmap.getCardinality() - 1) : null;
    }
    return null;
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return getColumnCapabilities(index, column);
  }

  @Override
  @Nullable
  public String getColumnTypeName(String columnName)
  {
    final ColumnHolder columnHolder = index.getColumnHolder(columnName);

    if (columnHolder == null) {
      return null;
    }

    try (final BaseColumn col = columnHolder.getColumn()) {
      if (col instanceof ComplexColumn) {
        return ((ComplexColumn) col).getTypeName();
      } else {
        return columnHolder.getCapabilities().getType().toString();
      }
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    // For immutable indexes, maxIngestedEventTime is maxTime.
    return getMaxTime();
  }

  @Override
  public boolean canVectorize(
      @Nullable final Filter filter,
      final VirtualColumns virtualColumns,
      final boolean descending
  )
  {
    if (filter != null) {
      final boolean filterCanVectorize =
          filter.shouldUseBitmapIndex(makeBitmapIndexSelector(virtualColumns))
          || filter.canVectorizeMatcher();

      if (!filterCanVectorize) {
        return false;
      }
    }

    // vector cursors can't iterate backwards yet
    return !descending;
  }

  @Override
  @Nullable
  public VectorCursor makeVectorCursor(
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final boolean descending,
      final int vectorSize,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    if (!canVectorize(filter, virtualColumns, descending)) {
      throw new ISE("Cannot vectorize. Check 'canVectorize' before calling 'makeVectorCursor'.");
    }

    if (queryMetrics != null) {
      queryMetrics.vectorized(true);
    }

    final Interval actualInterval = computeCursorInterval(Granularities.ALL, interval);

    if (actualInterval == null) {
      return null;
    }

    final ColumnSelectorBitmapIndexSelector bitmapIndexSelector = makeBitmapIndexSelector(virtualColumns);

    final FilterAnalysis filterAnalysis = analyzeFilter(filter, bitmapIndexSelector, queryMetrics);

    return new QueryableIndexCursorSequenceBuilder(
        index,
        actualInterval,
        virtualColumns,
        filterAnalysis.getPreFilterBitmap(),
        getMinTime().getMillis(),
        getMaxTime().getMillis(),
        descending,
        filterAnalysis.getPostFilter(),
        bitmapIndexSelector
    ).buildVectorized(vectorSize > 0 ? vectorSize : DEFAULT_VECTOR_SIZE);
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    if (queryMetrics != null) {
      queryMetrics.vectorized(false);
    }

    final Interval actualInterval = computeCursorInterval(gran, interval);

    if (actualInterval == null) {
      return Sequences.empty();
    }

    final ColumnSelectorBitmapIndexSelector bitmapIndexSelector = makeBitmapIndexSelector(virtualColumns);

    final FilterAnalysis filterAnalysis = analyzeFilter(filter, bitmapIndexSelector, queryMetrics);

    return Sequences.filter(
        new QueryableIndexCursorSequenceBuilder(
            index,
            actualInterval,
            virtualColumns,
            filterAnalysis.getPreFilterBitmap(),
            getMinTime().getMillis(),
            getMaxTime().getMillis(),
            descending,
            filterAnalysis.getPostFilter(),
            bitmapIndexSelector
        ).build(gran),
        Objects::nonNull
    );
  }

  @Nullable
  public static ColumnCapabilities getColumnCapabilities(ColumnSelector index, String columnName)
  {
    final ColumnHolder columnHolder = index.getColumnHolder(columnName);
    if (columnHolder == null) {
      return null;
    }
    return columnHolder.getCapabilities();
  }

  public static ColumnInspector getColumnInspectorForIndex(ColumnSelector index)
  {
    return new ColumnInspector()
    {
      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return QueryableIndexStorageAdapter.getColumnCapabilities(index, column);
      }
    };
  }

  @Override
  public Metadata getMetadata()
  {
    return index.getMetadata();
  }

  private void populateMinMaxTime()
  {
    // Compute and cache minTime, maxTime.
    final ColumnHolder columnHolder = index.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME);
    try (final NumericColumn column = (NumericColumn) columnHolder.getColumn()) {
      this.minTime = DateTimes.utc(column.getLongSingleValueRow(0));
      this.maxTime = DateTimes.utc(column.getLongSingleValueRow(column.length() - 1));
    }
  }

  @Nullable
  private Interval computeCursorInterval(final Granularity gran, final Interval interval)
  {
    final DateTime minTime = getMinTime();
    final DateTime maxTime = getMaxTime();
    final Interval dataInterval = new Interval(minTime, gran.bucketEnd(maxTime));

    if (!interval.overlaps(dataInterval)) {
      return null;
    }

    return interval.overlap(dataInterval);
  }

  @VisibleForTesting
  public ColumnSelectorBitmapIndexSelector makeBitmapIndexSelector(final VirtualColumns virtualColumns)
  {
    return new ColumnSelectorBitmapIndexSelector(
        index.getBitmapFactoryForDimensions(),
        virtualColumns,
        index
    );
  }

  @VisibleForTesting
  public FilterAnalysis analyzeFilter(
      @Nullable final Filter filter,
      ColumnSelectorBitmapIndexSelector indexSelector,
      @Nullable QueryMetrics queryMetrics
  )
  {
    final int totalRows = index.getNumRows();

    /*
     * Filters can be applied in two stages:
     * pre-filtering: Use bitmap indexes to prune the set of rows to be scanned.
     * post-filtering: Iterate through rows and apply the filter to the row values
     *
     * The pre-filter and post-filter step have an implicit AND relationship. (i.e., final rows are those that
     * were not pruned AND those that matched the filter during row scanning)
     *
     * An AND filter can have its subfilters partitioned across the two steps. The subfilters that can be
     * processed entirely with bitmap indexes (subfilter returns true for supportsBitmapIndex())
     * will be moved to the pre-filtering stage.
     *
     * Any subfilters that cannot be processed entirely with bitmap indexes will be moved to the post-filtering stage.
     */
    final List<Filter> preFilters;
    final List<Filter> postFilters = new ArrayList<>();
    int preFilteredRows = totalRows;
    if (filter == null) {
      preFilters = Collections.emptyList();
    } else {
      preFilters = new ArrayList<>();

      if (filter instanceof AndFilter) {
        // If we get an AndFilter, we can split the subfilters across both filtering stages
        for (Filter subfilter : ((AndFilter) filter).getFilters()) {

          if (subfilter.supportsBitmapIndex(indexSelector) && subfilter.shouldUseBitmapIndex(indexSelector)) {

            preFilters.add(subfilter);
          } else {
            postFilters.add(subfilter);
          }
        }
      } else {
        // If we get an OrFilter or a single filter, handle the filter in one stage
        if (filter.supportsBitmapIndex(indexSelector) && filter.shouldUseBitmapIndex(indexSelector)) {
          preFilters.add(filter);
        } else {
          postFilters.add(filter);
        }
      }
    }

    final ImmutableBitmap preFilterBitmap;
    if (preFilters.isEmpty()) {
      preFilterBitmap = null;
    } else {
      if (queryMetrics != null) {
        BitmapResultFactory<?> bitmapResultFactory =
            queryMetrics.makeBitmapResultFactory(indexSelector.getBitmapFactory());
        long bitmapConstructionStartNs = System.nanoTime();
        // Use AndFilter.getBitmapResult to intersect the preFilters to get its short-circuiting behavior.
        preFilterBitmap = AndFilter.getBitmapIndex(indexSelector, bitmapResultFactory, preFilters);
        preFilteredRows = preFilterBitmap.size();
        queryMetrics.reportBitmapConstructionTime(System.nanoTime() - bitmapConstructionStartNs);
      } else {
        BitmapResultFactory<?> bitmapResultFactory = new DefaultBitmapResultFactory(indexSelector.getBitmapFactory());
        preFilterBitmap = AndFilter.getBitmapIndex(indexSelector, bitmapResultFactory, preFilters);
      }
    }

    if (queryMetrics != null) {
      queryMetrics.preFilters(new ArrayList<>(preFilters));
      queryMetrics.postFilters(postFilters);
      queryMetrics.reportSegmentRows(totalRows);
      queryMetrics.reportPreFilteredRows(preFilteredRows);
    }

    return new FilterAnalysis(preFilterBitmap, Filters.maybeAnd(postFilters).orElse(null));
  }

  @VisibleForTesting
  public static class FilterAnalysis
  {
    private final Filter postFilter;
    private final ImmutableBitmap preFilterBitmap;

    public FilterAnalysis(
        @Nullable final ImmutableBitmap preFilterBitmap,
        @Nullable final Filter postFilter
    )
    {
      this.preFilterBitmap = preFilterBitmap;
      this.postFilter = postFilter;
    }

    @Nullable
    public ImmutableBitmap getPreFilterBitmap()
    {
      return preFilterBitmap;
    }

    @Nullable
    public Filter getPostFilter()
    {
      return postFilter;
    }
  }
}
