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

package org.apache.druid.query.rowsandcols.semantic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.ObjectBasedColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultFramedOnHeapAggregatable implements FramedOnHeapAggregatable
{
  private final AppendableRowsAndColumns rac;

  public DefaultFramedOnHeapAggregatable(AppendableRowsAndColumns rac)
  {
    this.rac = rac;
  }

  @Nonnull
  @Override
  public RowsAndColumns aggregateAll(
      WindowFrame frame,
      AggregatorFactory[] aggFactories
  )
  {
    if (frame.isLowerUnbounded() && frame.isUpperUnbounded()) {
      return computeUnboundedAggregates(aggFactories);
    }

    if (frame.getPeerType() == WindowFrame.PeerType.ROWS) {
      if (frame.isLowerUnbounded()) {
        return computeCumulativeAggregates(aggFactories, frame.getUpperOffset());
      } else if (frame.isUpperUnbounded()) {
        return computeReverseCumulativeAggregates(aggFactories, frame.getLowerOffset());
      } else {
        final int numRows = rac.numRows();
        int lowerOffset = frame.getLowerOffset();
        int upperOffset = frame.getUpperOffset();

        if (numRows < lowerOffset + upperOffset + 1) {
          // In this case, there are not enough rows to completely build up the full window aperture before it needs to
          // also start contracting the aperture because of the upper offset. So we use a method that specifically
          // handles checks for both expanding and reducing the aperture on every iteration.
          return aggregateWindowApertureInFlux(aggFactories, lowerOffset, upperOffset);
        } else {
          // In this case, there are 3 distinct phases that allow us to loop with less
          // branches, so we have a method that specifically does that.
          return aggregateWindowApertureWellBehaved(aggFactories, lowerOffset, upperOffset);
        }
      }
    } else {
      return computeGroupAggregates(aggFactories, frame);
    }
  }

  /**
   * Handles population/creation of new RAC columns.
   */
  static class ResultPopulator
  {

    private final Object[][] results;
    private final AggregatorFactory[] aggFactories;

    public ResultPopulator(AggregatorFactory[] aggFactories, int numRows)
    {
      this.aggFactories = aggFactories;
      results = new Object[aggFactories.length][numRows];
    }

    public void write(Interval outputRows, AggIntervalCursor aggCursor)
    {
      for (int col = 0; col < aggFactories.length; col++) {
        Arrays.fill(results[col], outputRows.a, outputRows.b, aggCursor.getValue(col));
      }
    }

    public void appendTo(AppendableRowsAndColumns rac)
    {
      for (int i = 0; i < aggFactories.length; ++i) {
        rac.addColumn(
            aggFactories[i].getName(),
            new ObjectArrayColumn(results[i], aggFactories[i].getIntermediateType())
        );
      }
    }
  }

  private RowsAndColumns computeGroupAggregates(
      AggregatorFactory[] aggFactories,
      WindowFrame frame)
  {
    Iterable<AggInterval> groupIterator = buildGroupIteratorFor(rac, frame);
    ResultPopulator resultRac = new ResultPopulator(aggFactories, rac.numRows());
    AggIntervalCursor aggCursor = new AggIntervalCursor(rac, aggFactories);
    for (AggInterval aggInterval : groupIterator) {
      aggCursor.moveTo(aggInterval.inputRows);
      resultRac.write(aggInterval.outputRows, aggCursor);
    }
    resultRac.appendTo(rac);
    return rac;
  }

  public static Iterable<AggInterval> buildGroupIteratorFor(AppendableRowsAndColumns rac, WindowFrame frame)
  {
    int[] groupBoundaries = ClusteredGroupPartitioner.fromRAC(rac).computeBoundaries(frame.getOrderByColNames());
    return new GroupIteratorForWindowFrame(frame, groupBoundaries);
  }

  static class GroupIteratorForWindowFrame implements Iterable<AggInterval>
  {
    private final int[] groupBoundaries;
    private final int numGroups;
    // lower inclusive
    private final int lowerOffset;
    // upper exclusive
    private final int upperOffset;

    public GroupIteratorForWindowFrame(WindowFrame frame, int[] groupBoundaries)
    {
      this.groupBoundaries = groupBoundaries;
      numGroups = groupBoundaries.length - 1;
      lowerOffset = frame.getLowerOffsetClamped(numGroups);
      upperOffset = Math.min(numGroups, frame.getUpperOffsetClamped(numGroups) + 1);
    }

    @Override
    public Iterator<AggInterval> iterator()
    {
      return new Iterator<AggInterval>()
      {
        int currentGroupIndex = 0;

        @Override
        public boolean hasNext()
        {
          return currentGroupIndex < numGroups;
        }

        @Override
        public AggInterval next()
        {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          AggInterval r = new AggInterval(
              Interval.of(
                  groupToRowIndex(relativeGroupId(0)),
                  groupToRowIndex(relativeGroupId(1))
              ),
              Interval.of(
                  groupToRowIndex(relativeGroupId(-lowerOffset)),
                  groupToRowIndex(relativeGroupId(upperOffset))
              )
          );

          currentGroupIndex++;
          return r;
        }

        private int groupToRowIndex(int groupId)
        {
          return groupBoundaries[groupId];
        }

        private int relativeGroupId(int groupOffset)
        {
          // invert iteration order at the end to get benefits of incremenental aggregations
          // for example if we have [0 BEFORE 1 AFTER]: for say 3 groups the order will be 0,2,1 instead of 0,1,2
          final int groupIndex = invertedOrderForLastK(currentGroupIndex, numGroups, upperOffset);

          int groupId = groupIndex + groupOffset;
          if (groupId < 0) {
            return 0;
          }
          if (groupId >= numGroups) {
            return numGroups;
          }
          return groupId;
        }
      };
    }
  }

  /**
   * Inverts order for the last K elements.
   *
   * For n=3, k=2 - it changes the iteration to 0,2,1
   */
  @VisibleForTesting
  public static int invertedOrderForLastK(int x, int n, int k)
  {
    Preconditions.checkState(k <= n);
    if (k <= 1 || x + k < n) {
      // we are in the non-interesting part
      return x;
    }
    int i = x - (n - k);
    return n - 1 - i;
  }

  /**
   * Basic [a,b) interval; left inclusive/right exclusive.
   */
  static class Interval implements Iterable<Integer>
  {
    final int a;
    final int b;

    public static Interval of(int a, int b)
    {
      return new Interval(a, b);
    }

    public Interval(int a, int b)
    {
      this.a = a;
      this.b = b;
    }

    @Override
    public Iterator<Integer> iterator()
    {
      return new Iterator<Integer>()
      {
        int current = a;

        @Override
        public Integer next()
        {
          if (!hasNext()) {
            throw new IllegalStateException();
          }
          return current++;
        }

        @Override
        public boolean hasNext()
        {
          return current < b;
        }
      };
    }

    @Override
    public String toString()
    {
      return StringUtils.format("Interval [%d ... %d[", a, b);
    }
  }

  /**
   * Represents an aggregation interval.
   *
   * Describes that the aggregation of {@link #inputRows} should be outputted to
   * all {@link #outputRows} specified.
   */
  static class AggInterval
  {
    final Interval outputRows;
    final Interval inputRows;

    public AggInterval(Interval outputRows, Interval inputRows)
    {
      this.outputRows = outputRows;
      this.inputRows = inputRows;
    }
  }

  public static Object cloneAggValue(AggregatorFactory aggFactory, Object value)
  {
    if (value == null || value instanceof Number) {
      // no need for the hussle
      return value;
    }
    Object[] currentValue = new Object[1];
    currentValue[0] = value;
    final CumulativeColumnSelectorFactory combiningFactory = new CumulativeColumnSelectorFactory(
        aggFactory,
        currentValue,
        0
    );
    Aggregator combiningAgg = aggFactory.getCombiningFactory().factorize(combiningFactory);

    combiningAgg.aggregate();
    return combiningAgg.get();
  }

  /**
   * Handles computations of aggregates for an {@link Interval}.
   *
   * Provides aggregates computed for a given {@link Interval}.
   * It could try to leverage earlier calculations internally if possible.
   */
  static class AggIntervalCursor
  {
    private AggregatorFactory[] aggFactories;
    private final AtomicInteger rowIdProvider;
    private final ColumnSelectorFactory columnSelectorFactory;

    /** Current interval the aggregators contain value for */
    private Interval currentRows = new Interval(0, 0);
    private final Aggregator[] aggregators;

    AggIntervalCursor(AppendableRowsAndColumns rac, AggregatorFactory[] aggFactories)
    {
      this.aggFactories = aggFactories;
      aggregators = new Aggregator[aggFactories.length];
      rowIdProvider = new AtomicInteger(0);
      columnSelectorFactory = ColumnSelectorFactoryMaker.fromRAC(rac).make(rowIdProvider);
      newAggregators();
    }

    public Object getValue(int aggIdx)
    {
      return cloneAggValue(aggFactories[aggIdx], aggregators[aggIdx].get());
    }

    /**
     * Reposition aggregation window to a new Interval.
     */
    public void moveTo(Interval newRows)
    {
      if (currentRows.a == newRows.a && currentRows.b < newRows.b) {
        // incremental addition of additional values
        for (int i = currentRows.b; i < newRows.b; i++) {
          aggregate(i);
        }
      } else {
        newAggregators();
        for (int i : newRows) {
          aggregate(i);
        }
      }
      currentRows = newRows;
    }

    private void newAggregators()
    {
      for (int i = 0; i < aggFactories.length; i++) {
        aggregators[i] = aggFactories[i].factorize(columnSelectorFactory);
      }
    }

    private void aggregate(int rowIdx)
    {
      rowIdProvider.set(rowIdx);
      for (int i = 0; i < aggFactories.length; i++) {
        aggregators[i].aggregate();
      }
    }
  }

  private AppendableRowsAndColumns computeUnboundedAggregates(AggregatorFactory[] aggFactories)
  {
    Aggregator[] aggs = new Aggregator[aggFactories.length];


    AtomicInteger currRow = new AtomicInteger(0);
    final ColumnSelectorFactory columnSelectorFactory = ColumnSelectorFactoryMaker.fromRAC(rac).make(currRow);

    for (int i = 0; i < aggFactories.length; i++) {
      aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
    }

    int numRows = rac.numRows();
    int rowId = currRow.get();
    while (rowId < numRows) {
      for (Aggregator agg : aggs) {
        agg.aggregate();
      }
      rowId = currRow.incrementAndGet();
    }

    for (int i = 0; i < aggFactories.length; ++i) {
      rac.addColumn(
          aggFactories[i].getName(),
          new ConstantObjectColumn(aggs[i].get(), numRows, aggFactories[i].getIntermediateType())
      );
      aggs[i].close();
    }
    return rac;
  }

  private AppendableRowsAndColumns computeCumulativeAggregates(AggregatorFactory[] aggFactories, int upperOffset)
  {
    int numRows = rac.numRows();
    if (upperOffset > numRows) {
      return computeUnboundedAggregates(aggFactories);
    }


    // We store the results in an Object array for convenience.  This is definitely sub-par from a memory management
    // point of view as we should use native arrays when possible.  This will be fine for now, but it probably makes
    // sense to look at optimizing this in the future.  That said, such an optimization might best come by having
    // a specialized implementation of this interface against, say, a Frame object that can deal with arrays instead
    // of trying to optimize this generic implementation.
    Object[][] results = new Object[aggFactories.length][numRows];
    int resultStorageIndex = 0;

    AtomicInteger rowIdProvider = new AtomicInteger(0);
    final ColumnSelectorFactory columnSelectorFactory = ColumnSelectorFactoryMaker.fromRAC(rac).make(rowIdProvider);

    AggregatorFactory[] combiningFactories = new AggregatorFactory[aggFactories.length];
    Aggregator[] aggs = new Aggregator[aggFactories.length];
    for (int i = 0; i < aggFactories.length; i++) {
      combiningFactories[i] = aggFactories[i].getCombiningFactory();
      aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
    }

    // If there is an upper offset, we accumulate those aggregations before starting to generate results
    for (int i = 0; i < upperOffset; ++i) {
      for (Aggregator agg : aggs) {
        agg.aggregate();
      }
      rowIdProvider.incrementAndGet();
    }

    // Prime the results
    if (rowIdProvider.get() < numRows) {
      for (int i = 0; i < aggs.length; i++) {
        aggs[i].aggregate();
        results[i][resultStorageIndex] = aggs[i].get();
        aggs[i].close();
        aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
      }

      ++resultStorageIndex;
      rowIdProvider.incrementAndGet();
    }

    // From here out, we want to aggregate, peel off a row of results and then accumulate the aggregation
    for (int rowId = rowIdProvider.get(); rowId < numRows; ++rowId) {
      for (int i = 0; i < aggs.length; i++) {
        aggs[i].aggregate();
        results[i][resultStorageIndex] = aggs[i].get();
        aggs[i].close();

        // Use a combining aggregator to combine the result we just got with the result from the previous row
        // This is a lot of hoops to jump through just to combine two values, but AggregatorFactory.combine
        // allows for mutation of either of the arguments passed in, so it cannot be meaningfully used in this
        // context.  Instead, we have to jump through these hoops to make sure that we are generating a new object.
        // It would've been nice if the AggregatorFactory interface had methods that were more usable for this,
        // but it doesn't so :shrug:
        final CumulativeColumnSelectorFactory combiningFactory = new CumulativeColumnSelectorFactory(
            aggFactories[i],
            results[i],
            resultStorageIndex - 1
        );
        final Aggregator combiningAgg = combiningFactories[i].factorize(combiningFactory);
        combiningAgg.aggregate();
        combiningFactory.increment();
        combiningAgg.aggregate();
        results[i][resultStorageIndex] = combiningAgg.get();
        combiningAgg.close();

        aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
      }

      ++resultStorageIndex;
      rowIdProvider.incrementAndGet();
    }

    // If we haven't filled up all of the results yet, there are no more rows, so just point the rest of the results
    // at the last result that we generated
    for (Object[] resultArr : results) {
      Arrays.fill(resultArr, resultStorageIndex, resultArr.length, resultArr[resultStorageIndex - 1]);
    }

    return makeReturnRAC(aggFactories, results);
  }

  private AppendableRowsAndColumns computeReverseCumulativeAggregates(AggregatorFactory[] aggFactories, int lowerOffset)
  {
    int numRows = rac.numRows();
    if (lowerOffset > numRows) {
      return computeUnboundedAggregates(aggFactories);
    }

    // We store the results in an Object array for convenience.  This is definitely sub-par from a memory management
    // point of view as we should use native arrays when possible.  This will be fine for now, but it probably makes
    // sense to look at optimizing this in the future.  That said, such an optimization might best come by having
    // a specialized implementation of this interface against, say, a Frame object that can deal with arrays instead
    // of trying to optimize this generic implementation.
    Object[][] results = new Object[aggFactories.length][numRows];
    int resultStorageIndex = numRows - 1;

    AtomicInteger rowIdProvider = new AtomicInteger(numRows - 1);
    final ColumnSelectorFactory columnSelectorFactory = ColumnSelectorFactoryMaker.fromRAC(rac).make(rowIdProvider);

    AggregatorFactory[] combiningFactories = new AggregatorFactory[aggFactories.length];
    Aggregator[] aggs = new Aggregator[aggFactories.length];
    for (int i = 0; i < aggFactories.length; i++) {
      combiningFactories[i] = aggFactories[i].getCombiningFactory();
      aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
    }

    // If there is a lower offset, we accumulate those aggregations before starting to generate results
    for (int i = 0; i < lowerOffset; ++i) {
      for (Aggregator agg : aggs) {
        agg.aggregate();
      }
      rowIdProvider.decrementAndGet();
    }

    // Prime the results
    if (rowIdProvider.get() >= 0) {
      for (int i = 0; i < aggs.length; i++) {
        aggs[i].aggregate();
        results[i][resultStorageIndex] = aggs[i].get();
        aggs[i].close();
        aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
      }

      --resultStorageIndex;
      rowIdProvider.decrementAndGet();
    }

    // From here out, we want to aggregate, peel off a row of results and then accumulate the aggregation
    for (int rowId = rowIdProvider.get(); rowId >= 0; --rowId) {
      for (int i = 0; i < aggs.length; i++) {
        aggs[i].aggregate();
        results[i][resultStorageIndex] = aggs[i].get();
        aggs[i].close();

        // Use a combining aggregator to combine the result we just got with the result from the previous row
        // This is a lot of hoops to jump through just to combine two values, but AggregatorFactory.combine
        // allows for mutation of either of the arguments passed in, so it cannot be meaningfully used in this
        // context.  Instead, we have to jump through these hoops to make sure that we are generating a new object.
        // It would've been nice if the AggregatorFactory interface had methods that were more usable for this,
        // but it doesn't so :shrug:
        final CumulativeColumnSelectorFactory combiningFactory = new CumulativeColumnSelectorFactory(
            aggFactories[i],
            results[i],
            resultStorageIndex + 1
        );
        final Aggregator combiningAgg = combiningFactories[i].factorize(combiningFactory);
        combiningAgg.aggregate();
        combiningFactory.decrement();
        combiningAgg.aggregate();
        results[i][resultStorageIndex] = combiningAgg.get();
        combiningAgg.close();

        aggs[i] = aggFactories[i].factorize(columnSelectorFactory);
      }

      --resultStorageIndex;
      rowIdProvider.decrementAndGet();
    }

    // If we haven't filled up all of the results yet, there are no more rows, so just point the rest of the results
    // at the last result that we generated
    for (Object[] resultArr : results) {
      Arrays.fill(resultArr, 0, resultStorageIndex + 1, resultArr[resultStorageIndex + 1]);
    }

    return makeReturnRAC(aggFactories, results);
  }

  private AppendableRowsAndColumns aggregateWindowApertureWellBehaved(
      AggregatorFactory[] aggFactories,
      int lowerOffset,
      int upperOffset
  )
  {
    // There are 3 different phases of operation when we have more rows than our window size
    // 1. Our window is not full, as we walk the rows we build up towards filling it
    // 2. Our window is full, as we walk the rows we take a value off and add a new aggregation
    // 3. We are nearing the end of the rows, we need to start shrinking the window aperture

    int numRows = rac.numRows();
    int windowSize = lowerOffset + upperOffset + 1;

    // We store the results in an Object array for convenience.  This is definitely sub-par from a memory management
    // point of view as we should use native arrays when possible.  This will be fine for now, but it probably makes
    // sense to look at optimizing this in the future.  That said, such an optimization might best come by having
    // a specialized implementation of this interface against, say, a Frame object that can deal with arrays instead
    // of trying to optimize this generic implementation.
    Object[][] results = new Object[aggFactories.length][numRows];
    int resultStorageIndex = 0;

    AtomicInteger rowIdProvider = new AtomicInteger(0);
    final ColumnSelectorFactory columnSelectorFactory = ColumnSelectorFactoryMaker.fromRAC(rac).make(rowIdProvider);

    // This is the number of aggregators to actually aggregate for the current row.
    // Which also doubles as the nextIndex to roll through as we roll things in and out of the window
    int nextIndex = lowerOffset + 1;

    Aggregator[][] aggregators = new Aggregator[aggFactories.length][windowSize];
    for (int i = 0; i < aggregators.length; i++) {
      final AggregatorFactory aggFactory = aggFactories[i];
      // instantiate the aggregators that need to be read on the first row.
      for (int j = 0; j < nextIndex; j++) {
        aggregators[i][j] = aggFactory.factorize(columnSelectorFactory);
      }
    }

    // The first few rows will slowly build out the window to consume the upper-offset.  The window will not
    // be full until we have walked upperOffset number of rows, so phase 1 runs until we have consumed
    // upperOffset number of rows.
    for (int upperIndex = 0; upperIndex < upperOffset; ++upperIndex) {
      for (Aggregator[] aggregator : aggregators) {
        for (int j = 0; j < nextIndex; ++j) {
          aggregator[j].aggregate();
        }
      }

      for (int i = 0; i < aggFactories.length; ++i) {
        aggregators[i][nextIndex] = aggFactories[i].factorize(columnSelectorFactory);
      }
      ++nextIndex;
      rowIdProvider.incrementAndGet();
    }

    // End Phase 1, Enter Phase 2.  At this point, nextIndex == windowSize, rowIdProvider is the same as
    // upperOffset and the aggregators matrix is entirely non-null.  We need to iterate until our window has all of
    // the aggregators in it to fill up the final result set.
    int endResultStorageIndex = numRows - windowSize;
    for (; resultStorageIndex < endResultStorageIndex; ++resultStorageIndex) {
      for (Aggregator[] aggregator : aggregators) {
        for (Aggregator value : aggregator) {
          value.aggregate();
        }
      }

      if (nextIndex == windowSize) {
        // Wrap back around and start pruning from the beginning of the window
        nextIndex = 0;
      }

      for (int i = 0; i < aggFactories.length; ++i) {
        results[i][resultStorageIndex] = aggregators[i][nextIndex].get();
        aggregators[i][nextIndex].close();
        aggregators[i][nextIndex] = aggFactories[i].factorize(columnSelectorFactory);
      }

      ++nextIndex;
      rowIdProvider.incrementAndGet();
    }

    if (nextIndex == windowSize) {
      nextIndex = 0;
    }

    // End Phase 2, enter Phase 3.  At this point, our window has enough aggregators in it to fill up our final
    // result set.  This means that for each new row that we complete, the window will "shrink" until we hit numRows,
    // at which point we will collect anything yet remaining and be done.

    if (nextIndex != 0) {
      // Start by organizing the aggregators so that we are 0-indexed from nextIndex.  This trades off creating
      // a new array of references in exchange for removing branches inside of the loop.  It also makes the logic
      // simpler to understand.

      Aggregator[][] reorganizedAggs = new Aggregator[aggFactories.length][windowSize];
      for (int i = 0; i < aggFactories.length; i++) {
        System.arraycopy(aggregators[i], nextIndex, reorganizedAggs[i], 0, windowSize - nextIndex);
        System.arraycopy(aggregators[i], 0, reorganizedAggs[i], windowSize - nextIndex, nextIndex);
      }
      aggregators = reorganizedAggs;
      nextIndex = 0;
    }

    for (int rowId = rowIdProvider.get(); rowId < numRows; ++rowId) {
      for (Aggregator[] aggregator : aggregators) {
        for (int j = nextIndex; j < aggregator.length; ++j) {
          aggregator[j].aggregate();
        }
      }

      for (int i = 0; i < aggFactories.length; ++i) {
        results[i][resultStorageIndex] = aggregators[i][nextIndex].get();
        aggregators[i][nextIndex].close();
        aggregators[i][nextIndex] = null;
      }

      ++nextIndex;
      ++resultStorageIndex;
      rowIdProvider.incrementAndGet();
    }

    // End Phase 3, anything left in the window needs to be collected and put into our results
    for (; nextIndex < windowSize; ++nextIndex) {
      for (int i = 0; i < aggFactories.length; ++i) {
        results[i][resultStorageIndex] = aggregators[i][nextIndex].get();
        aggregators[i][nextIndex] = null;
      }
      ++resultStorageIndex;
    }

    return makeReturnRAC(aggFactories, results);
  }

  private AppendableRowsAndColumns aggregateWindowApertureInFlux(
      AggregatorFactory[] aggFactories,
      int lowerOffset,
      int upperOffset
  )
  {
    // In this case, we need to store a value for all items, so our windowSize is equivalent to the number of rows
    // from the RowsAndColumns object that we are using.
    int windowSize = rac.numRows();

    // We store the results in an Object array for convenience.  This is definitely sub-par from a memory management
    // point of view as we should use native arrays when possible.  This will be fine for now, but it probably makes
    // sense to look at optimizing this in the future.  That said, such an optimization might best come by having
    // a specialized implementation of this interface against, say, a Frame object that can deal with arrays instead
    // of trying to optimize this generic implementation.
    Object[][] results = new Object[aggFactories.length][windowSize];
    int resultStorageIndex = 0;

    AtomicInteger rowIdProvider = new AtomicInteger(0);
    final ColumnSelectorFactory columnSelectorFactory = ColumnSelectorFactoryMaker.fromRAC(rac).make(rowIdProvider);

    Aggregator[][] aggregators = new Aggregator[aggFactories.length][windowSize];
    for (int i = 0; i < aggregators.length; i++) {
      final AggregatorFactory aggFactory = aggFactories[i];
      for (int j = 0; j < aggregators[i].length; j++) {
        aggregators[i][j] = aggFactory.factorize(columnSelectorFactory);
      }
    }

    // This is the index to stop at for the current window aperture
    // The first row is used by all of the results for the lowerOffset num results, plus 1 for the "current row"
    int stopIndex = Math.min(lowerOffset + 1, windowSize);

    int startIndex = 0;
    int rowId = rowIdProvider.get();
    while (rowId < windowSize) {
      for (Aggregator[] aggregator : aggregators) {
        for (int j = startIndex; j < stopIndex; ++j) {
          aggregator[j].aggregate();
        }
      }

      if (rowId >= upperOffset) {
        for (int i = 0; i < aggregators.length; ++i) {
          results[i][resultStorageIndex] = aggregators[i][startIndex].get();
          aggregators[i][startIndex].close();
          aggregators[i][startIndex] = null;
        }

        ++resultStorageIndex;
        ++startIndex;
      }

      if (stopIndex < windowSize) {
        ++stopIndex;
      }
      rowId = rowIdProvider.incrementAndGet();
    }


    for (; startIndex < windowSize; ++startIndex) {
      for (int i = 0; i < aggregators.length; ++i) {
        results[i][resultStorageIndex] = aggregators[i][startIndex].get();
        aggregators[i][startIndex].close();
        aggregators[i][startIndex] = null;
      }
      ++resultStorageIndex;
    }

    return makeReturnRAC(aggFactories, results);
  }

  private AppendableRowsAndColumns makeReturnRAC(AggregatorFactory[] aggFactories, Object[][] results)
  {
    for (int i = 0; i < aggFactories.length; ++i) {
      rac.addColumn(
          aggFactories[i].getName(), new ObjectArrayColumn(results[i], aggFactories[i].getIntermediateType())
      );
    }
    return rac;
  }

  private static class CumulativeColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final ColumnCapabilitiesImpl columnCapabilities;
    private final Object[] results;

    private int index;

    public CumulativeColumnSelectorFactory(AggregatorFactory factory, Object[] results, int initialIndex)
    {
      this.results = results;
      this.index = initialIndex;
      this.columnCapabilities = new ColumnCapabilitiesImpl()
          .setHasBitmapIndexes(false)
          .setDictionaryEncoded(false)
          .setHasMultipleValues(false)
          .setDictionaryValuesUnique(false)
          .setType(factory.getIntermediateType());
    }

    public void increment()
    {
      ++index;
    }

    public void decrement()
    {
      --index;
    }

    @Override
    @Nonnull
    public DimensionSelector makeDimensionSelector(@Nonnull DimensionSpec dimensionSpec)
    {
      throw new UOE("combining factory shouldn't need dimensions, just columnValue, dim[%s]", dimensionSpec);
    }

    @SuppressWarnings("rawtypes")
    @Override
    @Nonnull
    public ColumnValueSelector makeColumnValueSelector(@Nonnull String columnName)
    {
      return new ObjectBasedColumnSelector()
      {
        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {

        }

        @Nullable
        @Override
        public Object getObject()
        {
          return results[index];
        }

        @Override
        @Nonnull
        public Class classOfObject()
        {
          return results[index].getClass();
        }
      };
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(@Nonnull String column)
    {
      return columnCapabilities;
    }
  }
}
