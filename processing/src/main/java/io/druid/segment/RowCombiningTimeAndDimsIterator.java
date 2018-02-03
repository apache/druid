/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.query.aggregation.AggregateCombiner;
import io.druid.query.aggregation.AggregatorFactory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

/**
 * RowCombiningTimeAndDimsIterator takes some {@link RowIterator}s, assuming that they are "sorted" (see javadoc of
 * {@link MergingRowIterator} for the definition), merges the points as {@link MergingRowIterator}, and combines
 * all "equal" points (points which have the same time and dimension values) into one, using the provided metric
 * aggregator factories.
 */
final class RowCombiningTimeAndDimsIterator implements TimeAndDimsIterator
{
  private final MergingRowIterator mergingIterator;

  /**
   * Those pointers are used as {@link #currentTimeAndDimsPointer} (and therefore returned from {@link #getPointer()}),
   * until no points are actually combined. It's an optimization to reduce data movements from pointers of original
   * iterators to {@link #combinedTimeAndDimsPointersByOriginalIteratorIndex} on each iteration.
   */
  private final TimeAndDimsPointer[] markedRowPointersOfOriginalIterators;

  private final AggregateCombiner[] combinedMetricSelectors;

  private final List<String> combinedMetricNames;

  /**
   * We preserve as many "combined time and dims pointers" as there were original iterators. Each of them is a composite
   * of time and dimension selector from the original iterator by the corresponding index (see {@link
   * #makeCombinedPointer}), and the same metric selectors {@link #combinedMetricSelectors}. It allows to be
   * allocation-free during iteration, and also to reduce the number of any field writes during iteration.
   */
  private final TimeAndDimsPointer[] combinedTimeAndDimsPointersByOriginalIteratorIndex;

  private final BitSet currentIndexes = new BitSet();
  private final IntList[] rowsByIndex;

  @Nullable
  private TimeAndDimsPointer currentTimeAndDimsPointer;
  /**
   * If this field is soleCurrentPointSourceOriginalIteratorIndex >= 0, it means that no combines are done yet at the
   * current point, {@link #currentTimeAndDimsPointer} is one of {@link #markedRowPointersOfOriginalIterators}, and the
   * value of this field is the index of the original iterator which is the source of the sole (uncombined) point.
   *
   * If the value of this field is less than 0, it means that some combines are done at the current point, and {@link
   * #currentTimeAndDimsPointer} is one of {@link #combinedTimeAndDimsPointersByOriginalIteratorIndex}.
   */
  private int soleCurrentPointSourceOriginalIteratorIndex;

  @Nullable
  private RowPointer nextRowPointer;

  RowCombiningTimeAndDimsIterator(
      List<TransformableRowIterator> originalIterators,
      AggregatorFactory[] metricAggs,
      List<String> metricNames
  )
  {
    int numIndexes = originalIterators.size();
    mergingIterator = new MergingRowIterator(originalIterators);

    markedRowPointersOfOriginalIterators = new TimeAndDimsPointer[numIndexes];
    Arrays.setAll(
        markedRowPointersOfOriginalIterators,
        i -> {
          TransformableRowIterator originalIterator = mergingIterator.getOriginalIterator(i);
          return originalIterator != null ? originalIterator.getMarkedPointer() : null;
        }
    );

    combinedMetricSelectors = new AggregateCombiner[metricAggs.length];
    Arrays.setAll(combinedMetricSelectors, metricIndex -> metricAggs[metricIndex].makeAggregateCombiner());
    combinedMetricNames = metricNames;
    combinedTimeAndDimsPointersByOriginalIteratorIndex = new TimeAndDimsPointer[numIndexes];

    rowsByIndex = IntStream.range(0, numIndexes).mapToObj(i -> new IntArrayList()).toArray(IntList[]::new);

    if (mergingIterator.moveToNext()) {
      nextRowPointer = mergingIterator.getPointer();
    }
  }

  private void clear()
  {
    for (int i = currentIndexes.nextSetBit(0); i >= 0; i = currentIndexes.nextSetBit(i + 1)) {
      rowsByIndex[i].clear();
    }
    currentIndexes.clear();
  }

  @Override
  public boolean moveToNext()
  {
    clear();
    if (nextRowPointer == null) {
      currentTimeAndDimsPointer = null;
      return false;
    }
    startNewTimeAndDims(nextRowPointer);
    nextRowPointer = null;
    mergingIterator.mark();
    while (mergingIterator.moveToNext()) {
      if (mergingIterator.hasTimeAndDimsChangedSinceMark()) {
        nextRowPointer = mergingIterator.getPointer();
        return true;
      } else {
        combineToCurrentTimeAndDims(mergingIterator.getPointer());
      }
    }
    // No more rows left in mergingIterator
    nextRowPointer = null;
    return true;
  }

  /**
   * This method doesn't assign one of {@link #combinedTimeAndDimsPointersByOriginalIteratorIndex} into {@link
   * #currentTimeAndDimsPointer}, instead it uses one of {@link #markedRowPointersOfOriginalIterators}, see the javadoc
   * of the latter for explanation.
   */
  private void startNewTimeAndDims(RowPointer rowPointer)
  {
    int indexNum = rowPointer.getIndexNum();
    // Note: at the moment when this operation is performed, markedRowPointersOfOriginalIterators[indexNum] doesn't yet
    // contain the values that it should. startNewTimeAndDims() is called from moveToNext(), see above. Later in the
    // code of moveToNext(), mergingIterator.mark() is called, and then mergingIterator.moveToNext(). This will make
    // MergingRowIterator.moveToNext() implementation (see it's code) to call mark() on the current head iteratator,
    // and only after that markedRowPointersOfOriginalIterators[indexNum] will have correct values. So by the time
    // when moveToNext() (from where this method is called) exits, and before getPointer() could be called by the user
    // of this class, it will have correct values.
    currentTimeAndDimsPointer = markedRowPointersOfOriginalIterators[indexNum];
    soleCurrentPointSourceOriginalIteratorIndex = indexNum;
    currentIndexes.set(indexNum);
    rowsByIndex[indexNum].add(rowPointer.getRowNum());
  }

  private void combineToCurrentTimeAndDims(RowPointer rowPointer)
  {
    int soleCurrentPointSourceOriginalIteratorIndex = this.soleCurrentPointSourceOriginalIteratorIndex;
    if (soleCurrentPointSourceOriginalIteratorIndex >= 0) {
      TimeAndDimsPointer currentRowPointer = this.currentTimeAndDimsPointer;
      initCombinedCurrentPointer(currentRowPointer, soleCurrentPointSourceOriginalIteratorIndex);
      resetCombinedMetrics(currentRowPointer);
      this.soleCurrentPointSourceOriginalIteratorIndex = -1;
    }
    int indexNum = rowPointer.getIndexNum();
    currentIndexes.set(indexNum);
    rowsByIndex[indexNum].add(rowPointer.getRowNum());
    foldMetrics(rowPointer);
  }

  private void initCombinedCurrentPointer(TimeAndDimsPointer currentRowPointer, int indexNum)
  {
    currentTimeAndDimsPointer = combinedTimeAndDimsPointersByOriginalIteratorIndex[indexNum];
    if (currentTimeAndDimsPointer == null) {
      currentTimeAndDimsPointer = makeCombinedPointer(currentRowPointer, indexNum);
      combinedTimeAndDimsPointersByOriginalIteratorIndex[indexNum] = currentTimeAndDimsPointer;
    }
  }

  private TimeAndDimsPointer makeCombinedPointer(TimeAndDimsPointer currentRowPointer, int indexNum)
  {
    return new TimeAndDimsPointer(
        markedRowPointersOfOriginalIterators[indexNum].timestampSelector,
        markedRowPointersOfOriginalIterators[indexNum].dimensionSelectors,
        currentRowPointer.getDimensionHandlers(),
        combinedMetricSelectors,
        combinedMetricNames
    );
  }

  private void resetCombinedMetrics(TimeAndDimsPointer currentRowPointer)
  {
    for (int metricIndex = 0; metricIndex < combinedMetricSelectors.length; metricIndex++) {
      combinedMetricSelectors[metricIndex].reset(currentRowPointer.getMetricSelector(metricIndex));
    }
  }

  private void foldMetrics(RowPointer rowPointer)
  {
    for (int metricIndex = 0; metricIndex < combinedMetricSelectors.length; metricIndex++) {
      combinedMetricSelectors[metricIndex].fold(rowPointer.getMetricSelector(metricIndex));
    }
  }

  @Override
  public TimeAndDimsPointer getPointer()
  {
    return Objects.requireNonNull(currentTimeAndDimsPointer);
  }

  /**
   * Gets the next index of iterators (as provided in the List in constructor of RowCombiningTimeAndDimsIterator),
   * that was the source of one or more points, that are combined to produce the current {@link #getPointer()} point.
   *
   * Should be used a-la {@link BitSet} iteration:
   * for (int origItIndex = nextCurrentlyCombinedOriginalIteratorIndex(0);
   *     origItIndex >= 0;
   *     origItIndex = nextCurrentlyCombinedOriginalIteratorIndex(origItIndex + 1)) {
   *   ...
   * }
   */
  int nextCurrentlyCombinedOriginalIteratorIndex(int fromIndex)
  {
    return currentIndexes.nextSetBit(fromIndex);
  }

  /**
   * Gets the numbers of rows in the original iterator with the given index, that are combined to produce the current
   * {@link #getPointer()} point.
   */
  IntList getCurrentlyCombinedRowNumsByOriginalIteratorIndex(int originalIteratorIndex)
  {
    return rowsByIndex[originalIteratorIndex];
  }

  @Override
  public void close()
  {
    mergingIterator.close();
  }
}
