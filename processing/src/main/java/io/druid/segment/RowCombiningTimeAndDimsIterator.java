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

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * RowCombiningTimeAndDimsIterator takes some {@link RowIterator}s, assuming that they are "sorted" (see javadoc of
 * {@link MergingRowIterator} for the definition), merges the points as {@link MergingRowIterator}, and combines
 * all "equal" points (points which have the same time and dimension values) into one, using the provided metric
 * aggregator factories.
 */
final class RowCombiningTimeAndDimsIterator implements TimeAndDimsIterator
{
  private static final int MIN_CURRENTLY_COMBINED_ROW_NUM_UNSET_VALUE = -1;

  private final MergingRowIterator mergingIterator;

  /**
   * Those pointers are used as {@link #currentTimeAndDimsPointer} (and therefore returned from {@link #getPointer()}),
   * until there is just one point in the "equivalence class" to be combined. It's an optimization: alternative was to
   * start to "combine" right away for each point, and thus use only {@link
   * #combinedTimeAndDimsPointersByOriginalIteratorIndex}. This optimization aims to reduce data movements from pointers
   * of original iterators to {@link #combinedTimeAndDimsPointersByOriginalIteratorIndex} on each iteration.
   *
   * @see #soleCurrentPointSourceOriginalIteratorIndex
   */
  private final TimeAndDimsPointer[] markedRowPointersOfOriginalIterators;

  private final AggregateCombiner[] combinedMetricSelectors;

  private final List<String> combinedMetricNames;

  /**
   * We preserve as many "combined time and dims pointers" as there were original iterators. Each of them is a composite
   * of time and dimension selectors from the original iterator by the corresponding index, and the same metric
   * selectors ({@link #combinedMetricSelectors}). It allows to be allocation-free during iteration, and also to reduce
   * the number of field writes during iteration.
   */
  private final TimeAndDimsPointer[] combinedTimeAndDimsPointersByOriginalIteratorIndex;

  /**
   * This bitset has set bits that correspond to the indexes of the original iterators, that participate the current
   * combination of all points from the current "equivalence class", resulting in the current {@link #getPointer()}
   * point.
   *
   * If there are less than 64 iterators combined, this field could be optimized to be just a single primitive long.
   * This optimization could be done in the future.
   */
  private final BitSet indexesOfCurrentlyCombinedOriginalIterators = new BitSet();

  /**
   * This field and {@link #maxCurrentlyCombinedRowNumByOriginalIteratorIndex} designate "row num range" in each
   * original iterator, points from which are currently combined (see {@link
   * #indexesOfCurrentlyCombinedOriginalIterators}). It could have been a single row number, if original iterators were
   * guaranteed to have no duplicate rows themselves, but they are not.
   */
  private final int[] minCurrentlyCombinedRowNumByOriginalIteratorIndex;
  private final int[] maxCurrentlyCombinedRowNumByOriginalIteratorIndex;

  @Nullable
  private TimeAndDimsPointer currentTimeAndDimsPointer;

  /**
   * If soleCurrentPointSourceOriginalIteratorIndex >= 0, it means that no combines are done yet at the current point,
   * {@link #currentTimeAndDimsPointer} is one of {@link #markedRowPointersOfOriginalIterators}, and the value of this
   * field is the index of the original iterator which is the source of the sole (uncombined) point.
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
    int numCombinedIterators = originalIterators.size();
    mergingIterator = new MergingRowIterator(originalIterators);

    markedRowPointersOfOriginalIterators = new TimeAndDimsPointer[numCombinedIterators];
    Arrays.setAll(
        markedRowPointersOfOriginalIterators,
        originalIteratorIndex -> {
          TransformableRowIterator originalIterator = mergingIterator.getOriginalIterator(originalIteratorIndex);
          return originalIterator != null ? originalIterator.getMarkedPointer() : null;
        }
    );

    combinedMetricSelectors = new AggregateCombiner[metricAggs.length];
    Arrays.setAll(combinedMetricSelectors, metricIndex -> metricAggs[metricIndex].makeAggregateCombiner());
    combinedMetricNames = metricNames;

    combinedTimeAndDimsPointersByOriginalIteratorIndex = new TimeAndDimsPointer[numCombinedIterators];
    Arrays.setAll(
        combinedTimeAndDimsPointersByOriginalIteratorIndex,
        originalIteratorIndex -> {
          TimeAndDimsPointer markedRowPointer = markedRowPointersOfOriginalIterators[originalIteratorIndex];
          if (markedRowPointer != null) {
            return new TimeAndDimsPointer(
                markedRowPointer.timestampSelector,
                markedRowPointer.dimensionSelectors,
                markedRowPointer.getDimensionHandlers(),
                combinedMetricSelectors,
                combinedMetricNames
            );
          } else {
            return null;
          }
        }
    );

    minCurrentlyCombinedRowNumByOriginalIteratorIndex = new int[numCombinedIterators];
    Arrays.fill(minCurrentlyCombinedRowNumByOriginalIteratorIndex, MIN_CURRENTLY_COMBINED_ROW_NUM_UNSET_VALUE);
    maxCurrentlyCombinedRowNumByOriginalIteratorIndex = new int[numCombinedIterators];

    if (mergingIterator.moveToNext()) {
      nextRowPointer = mergingIterator.getPointer();
    }
  }

  /**
   * Clear the info about which rows (in which original iterators and which row nums within them) were combined on
   * the previous step.
   */
  private void clearCombinedRowsInfo()
  {
    for (int originalIteratorIndex = indexesOfCurrentlyCombinedOriginalIterators.nextSetBit(0);
         originalIteratorIndex >= 0;
         originalIteratorIndex = indexesOfCurrentlyCombinedOriginalIterators.nextSetBit(originalIteratorIndex + 1)) {
      minCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex] =
          MIN_CURRENTLY_COMBINED_ROW_NUM_UNSET_VALUE;
    }
    indexesOfCurrentlyCombinedOriginalIterators.clear();
  }

  /**
   * Warning: this method and {@link #startNewTimeAndDims} have just ~25 lines of code, but their logic is very
   * convoluted and hard to understand. It could be especially confusing to try to understand it via debug.
   */
  @Override
  public boolean moveToNext()
  {
    clearCombinedRowsInfo();
    if (nextRowPointer == null) {
      currentTimeAndDimsPointer = null;
      return false;
    }
    // This line implicitly uses the property of RowIterator.getPointer() (see [*] below), that it's still valid after
    // RowPointer.moveToNext() returns false. mergingIterator.moveToNext() could have returned false during the previous
    // call to this method, RowCombiningTimeAndDimsIterator.moveToNext().
    startNewTimeAndDims(nextRowPointer);
    nextRowPointer = null;
    // [1] -- see comment in startNewTimeAndDims()
    mergingIterator.mark();
    // [2] -- see comment in startNewTimeAndDims()
    while (mergingIterator.moveToNext()) {
      if (mergingIterator.hasTimeAndDimsChangedSinceMark()) {
        nextRowPointer = mergingIterator.getPointer(); // [*]
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
   * of this field for explanation.
   */
  private void startNewTimeAndDims(RowPointer rowPointer)
  {
    int originalIteratorIndex = rowPointer.getIndexNum();
    // Note: at the moment when this operation is performed, markedRowPointersOfOriginalIterators[originalIteratorIndex]
    // doesn't yet contain actual current dimension and metric values. startNewTimeAndDims() is called from
    // moveToNext(), see above. Later in the code of moveToNext(), mergingIterator.mark() [1] is called, and then
    // mergingIterator.moveToNext() [2]. This will make MergingRowIterator.moveToNext() implementation (see it's code)
    // to call mark() on the current head iteratator, and only after that
    // markedRowPointersOfOriginalIterators[originalIteratorIndex] will have correct values. So by the time when
    // moveToNext() (from where this method is called) exits, and before getPointer() could be called by the user of
    // this class, it will have correct values.
    currentTimeAndDimsPointer = markedRowPointersOfOriginalIterators[originalIteratorIndex];
    soleCurrentPointSourceOriginalIteratorIndex = originalIteratorIndex;
    indexesOfCurrentlyCombinedOriginalIterators.set(originalIteratorIndex);
    int rowNum = rowPointer.getRowNum();
    minCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex] = rowNum;
    maxCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex] = rowNum;
  }

  private void combineToCurrentTimeAndDims(RowPointer rowPointer)
  {
    int soleCurrentPointSourceOriginalIteratorIndex = this.soleCurrentPointSourceOriginalIteratorIndex;
    if (soleCurrentPointSourceOriginalIteratorIndex >= 0) {
      TimeAndDimsPointer currentRowPointer = this.currentTimeAndDimsPointer;
      assert currentRowPointer != null;
      resetCombinedMetrics(currentRowPointer);
      currentTimeAndDimsPointer =
          combinedTimeAndDimsPointersByOriginalIteratorIndex[soleCurrentPointSourceOriginalIteratorIndex];
      this.soleCurrentPointSourceOriginalIteratorIndex = -1;
    }

    int originalIteratorIndex = rowPointer.getIndexNum();
    indexesOfCurrentlyCombinedOriginalIterators.set(originalIteratorIndex);
    int rowNum = rowPointer.getRowNum();
    if (minCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex] < 0) {
      minCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex] = rowNum;
    }
    maxCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex] = rowNum;

    foldMetrics(rowPointer);
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
   * for (int originalIteratorIndex = nextCurrentlyCombinedOriginalIteratorIndex(0);
   *     originalIteratorIndex >= 0;
   *     originalIteratorIndex = nextCurrentlyCombinedOriginalIteratorIndex(originalIteratorIndex + 1)) {
   *   ...
   * }
   */
  int nextCurrentlyCombinedOriginalIteratorIndex(int fromIndex)
  {
    return indexesOfCurrentlyCombinedOriginalIterators.nextSetBit(fromIndex);
  }

  /**
   * See Javadoc of {@link #minCurrentlyCombinedRowNumByOriginalIteratorIndex} for explanation.
   */
  int getMinCurrentlyCombinedRowNumByOriginalIteratorIndex(int originalIteratorIndex)
  {
    return minCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex];
  }

  /**
   * See Javadoc of {@link #minCurrentlyCombinedRowNumByOriginalIteratorIndex} for explanation.
   */
  int getMaxCurrentlyCombinedRowNumByOriginalIteratorIndex(int originalIteratorIndex)
  {
    return maxCurrentlyCombinedRowNumByOriginalIteratorIndex[originalIteratorIndex];
  }

  @Override
  public void close()
  {
    mergingIterator.close();
  }
}
