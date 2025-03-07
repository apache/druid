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
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.operator.window.WindowFrame;
import org.apache.druid.query.operator.window.WindowFrame.Groups;
import org.apache.druid.query.operator.window.WindowFrame.OffsetFrame;
import org.apache.druid.query.operator.window.WindowFrame.Rows;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
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
    Iterable<AggInterval> groupIterator = buildIteratorFor(rac, frame);
    ResultPopulator resultRac = new ResultPopulator(aggFactories, rac.numRows());
    AggIntervalCursor aggCursor = new AggIntervalCursor(rac, aggFactories);
    for (AggInterval aggInterval : groupIterator) {
      aggCursor.moveTo(aggInterval.inputRows);
      resultRac.write(aggInterval.outputRows, aggCursor);
    }
    resultRac.appendTo(rac);
    return rac;
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

  public static Iterable<AggInterval> buildIteratorFor(AppendableRowsAndColumns rac, WindowFrame frame)
  {
    int numRows = rac.numRows();
    if (isEffectivelyUnbounded(frame, numRows)) {
      return buildUnboundedIteratorFor(rac);
    }
    Rows rowsFrame = frame.unwrap(WindowFrame.Rows.class);
    if (rowsFrame != null) {
      return buildRowIteratorFor(rac, rowsFrame);
    }
    Groups groupsFrame = frame.unwrap(WindowFrame.Groups.class);
    if (groupsFrame != null) {
      return buildGroupIteratorFor(rac, groupsFrame);
    }
    throw DruidException.defensive("Unable to handle WindowFrame [%s]!", frame);
  }

  private static boolean isEffectivelyUnbounded(WindowFrame frame, int numRows)
  {
    OffsetFrame offsetFrame = frame.unwrap(WindowFrame.OffsetFrame.class);
    if (offsetFrame.getLowerOffsetClamped(numRows) == -numRows
        && offsetFrame.getUpperOffsetClamped(numRows) == numRows) {
      // regardless the actual mode; all rows will be inside the frame!
      return true;
    }
    return false;
  }

  private static Iterable<AggInterval> buildUnboundedIteratorFor(AppendableRowsAndColumns rac)
  {
    int[] groupBoundaries = new int[] {0, rac.numRows()};
    return new GroupIteratorForWindowFrame(WindowFrame.rows(null, null), groupBoundaries);
  }

  private static Iterable<AggInterval> buildRowIteratorFor(AppendableRowsAndColumns rac, WindowFrame.Rows frame)
  {
    int[] groupBoundaries = new int[rac.numRows() + 1];
    for (int j = 0; j < groupBoundaries.length; j++) {
      groupBoundaries[j] = j;
    }
    return new GroupIteratorForWindowFrame(frame, groupBoundaries);
  }

  private static Iterable<AggInterval> buildGroupIteratorFor(AppendableRowsAndColumns rac, WindowFrame.Groups frame)
  {
    int[] groupBoundaries = ClusteredGroupPartitioner.fromRAC(rac).computeBoundaries(frame.getOrderByColumns());
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

    public GroupIteratorForWindowFrame(WindowFrame.OffsetFrame frame, int[] groupBoundaries)
    {
      this.groupBoundaries = groupBoundaries;
      numGroups = groupBoundaries.length - 1;
      lowerOffset = frame.getLowerOffsetClamped(numGroups);
      upperOffset = Math.min(numGroups, frame.getUpperOffsetClamped(numGroups) + 1);
    }

    @Override
    public Iterator<AggInterval> iterator()
    {
      return new Iterator<>()
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
                  groupToRowIndex(relativeGroupId(lowerOffset)),
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
      return new Iterator<>()
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

    try (Aggregator combiningAgg = aggFactory.getCombiningFactory().factorize(combiningFactory)) {
      combiningAgg.aggregate();
      return aggFactory.finalizeComputation(combiningAgg.get());
    }
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
      } else if (currentRows.a > newRows.a && currentRows.b == newRows.b) {
        for (int i = newRows.a; i < currentRows.a; i++) {
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
