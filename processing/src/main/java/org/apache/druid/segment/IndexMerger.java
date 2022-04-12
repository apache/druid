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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.ImplementedBy;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@ImplementedBy(IndexMergerV9.class)
public interface IndexMerger
{
  Logger log = new Logger(IndexMerger.class);

  SerializerUtils SERIALIZER_UTILS = new SerializerUtils();
  int INVALID_ROW = -1;
  int UNLIMITED_MAX_COLUMNS_TO_MERGE = -1;

  static List<String> getMergedDimensionsFromQueryableIndexes(
      List<QueryableIndex> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    return getMergedDimensions(toIndexableAdapters(indexes), dimensionsSpec);
  }

  static List<IndexableAdapter> toIndexableAdapters(List<QueryableIndex> indexes)
  {
    return indexes.stream().map(QueryableIndexIndexableAdapter::new).collect(Collectors.toList());
  }

  static List<String> getMergedDimensions(
      List<IndexableAdapter> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    if (indexes.size() == 0) {
      return ImmutableList.of();
    }
    List<String> commonDimOrder = getLongestSharedDimOrder(indexes, dimensionsSpec);
    if (commonDimOrder == null) {
      log.warn("Indexes have incompatible dimension orders and there is no valid dimension ordering"
               + " in the ingestionSpec, using lexicographic order.");
      return getLexicographicMergedDimensions(indexes);
    } else {
      return commonDimOrder;
    }
  }

  @Nullable
  static List<String> getLongestSharedDimOrder(
      List<IndexableAdapter> indexes,
      @Nullable DimensionsSpec dimensionsSpec
  )
  {
    int maxSize = 0;
    Iterable<String> orderingCandidate = null;
    for (IndexableAdapter index : indexes) {
      int iterSize = index.getDimensionNames().size();
      if (iterSize > maxSize) {
        maxSize = iterSize;
        orderingCandidate = index.getDimensionNames();
      }
    }

    if (orderingCandidate == null) {
      return null;
    }

    if (isDimensionOrderingValid(indexes, orderingCandidate)) {
      return ImmutableList.copyOf(orderingCandidate);
    } else {
      log.info("Indexes have incompatible dimension orders, try falling back on dimension ordering from ingestionSpec");
      // Check if there is a valid dimension ordering in the ingestionSpec to fall back on
      if (dimensionsSpec == null || CollectionUtils.isNullOrEmpty(dimensionsSpec.getDimensionNames())) {
        log.info("Cannot fall back on dimension ordering from ingestionSpec as it does not exist");
        return null;
      }
      List<String> candidate = new ArrayList<>(dimensionsSpec.getDimensionNames());
      // Remove all dimensions that does not exist within the indexes from the candidate
      Set<String> allValidDimensions = indexes.stream()
                                         .flatMap(indexableAdapter -> indexableAdapter.getDimensionNames().stream())
                                         .collect(Collectors.toSet());
      candidate.retainAll(allValidDimensions);
      // Sanity check that there is no extra/missing columns
      if (candidate.size() != allValidDimensions.size()) {
        log.error("Dimension mismatched between ingestionSpec and indexes. ingestionSpec[%s] indexes[%s]",
                  candidate,
                  allValidDimensions);
        return null;
      }

      // Sanity check that all indexes dimension ordering is the same as the ordering in candidate
      if (!isDimensionOrderingValid(indexes, candidate)) {
        log.error("Dimension from ingestionSpec has invalid ordering");
        return null;
      }
      log.info("Dimension ordering from ingestionSpec is valid. Fall back on dimension ordering [%s]", candidate);
      return candidate;
    }
  }

  static boolean isDimensionOrderingValid(List<IndexableAdapter> indexes, Iterable<String> orderingCandidate)
  {
    for (IndexableAdapter index : indexes) {
      Iterator<String> candidateIter = orderingCandidate.iterator();
      for (String matchDim : index.getDimensionNames()) {
        boolean matched = false;
        while (candidateIter.hasNext()) {
          String nextDim = candidateIter.next();
          if (matchDim.equals(nextDim)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          return false;
        }
      }
    }
    return true;
  }

  static List<String> getLexicographicMergedDimensions(List<IndexableAdapter> indexes)
  {
    return mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return input.getDimensionNames();
              }
            }
        )
    );
  }

  static <T extends Comparable<? super T>> ArrayList<T> mergeIndexed(List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = new TreeSet<>(Comparators.naturalNullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  /**
   * Equivalent to {@link #persist(IncrementalIndex, Interval, File, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory)}
   * without a progress indicator and with interval set to {@link IncrementalIndex#getInterval()}.
   */
  @VisibleForTesting
  default File persist(
      IncrementalIndex index,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return persist(
        index,
        index.getInterval(),
        outDir,
        indexSpec,
        new BaseProgressIndicator(),
        segmentWriteOutMediumFactory
    );
  }

  /**
   * Equivalent to {@link #persist(IncrementalIndex, Interval, File, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory)}
   * without a progress indicator.
   */
  default File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    return persist(index, dataInterval, outDir, indexSpec, new BaseProgressIndicator(), segmentWriteOutMediumFactory);
  }

  /**
   * Persist an IncrementalIndex to disk in such a way that it can be loaded back up as a {@link QueryableIndex}.
   *
   * This is *not* thread-safe and havoc will ensue if this is called and writes are still occurring on the
   * IncrementalIndex object.
   *
   * @param index                        the IncrementalIndex to persist
   * @param dataInterval                 the Interval that the data represents. Typically, this is the same as the
   *                                     interval from the corresponding {@link org.apache.druid.timeline.SegmentId}.
   * @param outDir                       the directory to persist the data to
   * @param indexSpec                    storage and compression options
   * @param progress                     an object that will receive progress updates
   * @param segmentWriteOutMediumFactory controls allocation of temporary data structures
   *
   * @return the index output directory
   *
   * @throws IOException if an IO error occurs persisting the index
   */
  File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException;

  /**
   * Merge a collection of {@link QueryableIndex}.
   *
   * Only used as a convenience method in tests. In production code, use the full version
   * {@link #mergeQueryableIndex(List, boolean, AggregatorFactory[], DimensionsSpec, File, IndexSpec, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory, int)}.
   */
  @VisibleForTesting
  default File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException
  {
    return mergeQueryableIndex(
        indexes,
        rollup,
        metricAggs,
        null,
        outDir,
        indexSpec,
        indexSpec,
        new BaseProgressIndicator(),
        segmentWriteOutMediumFactory,
        maxColumnsToMerge
    );
  }

  /**
   * Merge a collection of {@link QueryableIndex}.
   */
  File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      @Nullable DimensionsSpec dimensionsSpec,
      File outDir,
      IndexSpec indexSpec,
      IndexSpec indexSpecForIntermediatePersists,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      int maxColumnsToMerge
  ) throws IOException;

  /**
   * Only used as a convenience method in tests.
   *
   * In production code, to merge multiple {@link QueryableIndex}, use
   * {@link #mergeQueryableIndex(List, boolean, AggregatorFactory[], DimensionsSpec, File, IndexSpec, IndexSpec, ProgressIndicator, SegmentWriteOutMediumFactory, int)}.
   * To merge multiple {@link IncrementalIndex}, call one of the {@link #persist} methods and then merge the resulting
   * {@link QueryableIndex}.
   */
  @VisibleForTesting
  File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      DimensionsSpec dimensionsSpec,
      IndexSpec indexSpec,
      int maxColumnsToMerge
  ) throws IOException;

  /**
   * This method applies {@link DimensionMerger#convertSortedSegmentRowValuesToMergedRowValues(int, ColumnValueSelector)} to
   * all dimension column selectors of the given sourceRowIterator, using the given index number.
   */
  static TransformableRowIterator toMergedIndexRowIterator(
      TransformableRowIterator sourceRowIterator,
      int indexNumber,
      final List<DimensionMergerV9> mergers
  )
  {
    RowPointer sourceRowPointer = sourceRowIterator.getPointer();
    TimeAndDimsPointer markedSourceRowPointer = sourceRowIterator.getMarkedPointer();
    boolean anySelectorChanged = false;
    ColumnValueSelector[] convertedDimensionSelectors = new ColumnValueSelector[mergers.size()];
    ColumnValueSelector[] convertedMarkedDimensionSelectors = new ColumnValueSelector[mergers.size()];
    for (int i = 0; i < mergers.size(); i++) {
      ColumnValueSelector sourceDimensionSelector = sourceRowPointer.getDimensionSelector(i);
      ColumnValueSelector convertedDimensionSelector =
          mergers.get(i).convertSortedSegmentRowValuesToMergedRowValues(indexNumber, sourceDimensionSelector);
      convertedDimensionSelectors[i] = convertedDimensionSelector;
      // convertedDimensionSelector could be just the same object as sourceDimensionSelector, it means that this
      // type of column doesn't have any kind of special per-index encoding that needs to be converted to the "global"
      // encoding. E. g. it's always true for subclasses of NumericDimensionMergerV9.
      //noinspection ObjectEquality
      anySelectorChanged |= convertedDimensionSelector != sourceDimensionSelector;

      convertedMarkedDimensionSelectors[i] = mergers.get(i).convertSortedSegmentRowValuesToMergedRowValues(
          indexNumber,
          markedSourceRowPointer.getDimensionSelector(i)
      );
    }
    // If none dimensions are actually converted, don't need to transform the sourceRowIterator, adding extra
    // indirection layer. It could be just returned back from this method.
    if (!anySelectorChanged) {
      return sourceRowIterator;
    }
    return makeRowIteratorWithConvertedDimensionColumns(
        sourceRowIterator,
        convertedDimensionSelectors,
        convertedMarkedDimensionSelectors
    );
  }

  static TransformableRowIterator makeRowIteratorWithConvertedDimensionColumns(
      TransformableRowIterator sourceRowIterator,
      ColumnValueSelector[] convertedDimensionSelectors,
      ColumnValueSelector[] convertedMarkedDimensionSelectors
  )
  {
    RowPointer convertedRowPointer = sourceRowIterator.getPointer().withDimensionSelectors(convertedDimensionSelectors);
    TimeAndDimsPointer convertedMarkedRowPointer =
        sourceRowIterator.getMarkedPointer().withDimensionSelectors(convertedMarkedDimensionSelectors);
    return new ForwardingRowIterator(sourceRowIterator)
    {
      @Override
      public RowPointer getPointer()
      {
        return convertedRowPointer;
      }

      @Override
      public TimeAndDimsPointer getMarkedPointer()
      {
        return convertedMarkedRowPointer;
      }
    };
  }
}
