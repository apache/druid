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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.common.ProcessingTestToolbox;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.data.gen.TestColumnSchema;
import org.apache.druid.data.gen.TestDataGenerator;
import org.apache.druid.data.gen.TestSchemaInfo;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema.ValueType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class AggregateTestBase extends InitializedNullHandlingTest
{
  // float, double, long, single-valued string, multi-valued string w/o nulls
  public enum TestColumn
  {
    FLOAT_COLUMN("floatColumn", ValueType.FLOAT),
    DOUBLE_COLUMN("doubleColumn", ValueType.DOUBLE),
    LONG_COLUMN("longColumn", ValueType.LONG),
    SINGLE_VALUED_STRING_COLUMN("singleValuedStringColumn", ValueType.STRING),
    MULTI_VALUED_STRING_COLUMN("multiValuedStringColumn", ValueType.STRING);

    private final String name;
    private final ValueType valueType;

    TestColumn(String name, ValueType valueType)
    {
      this.name = name;
      this.valueType = valueType;
    }

    public String getName()
    {
      return name;
    }

    public ValueType getValueType()
    {
      return valueType;
    }

    public static TestColumn getColumn(String columnName)
    {
      return Arrays.stream(TestColumn.values())
                   .filter(col -> columnName.equals(col.getName()))
                   .findFirst()
                   .orElseThrow(() -> new IAE("Unknown column: %s", columnName));
    }
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final TestDataGenerator dataGenerator;
  private final TestSchemaInfo testSchemaInfo;
  private final int numTimePartitions;
  private final int numRowsPerTimePartition;
  private final boolean rollup;
  private final boolean persist;

  private IndexHolder indexHolder;

  public AggregateTestBase(
      Interval interval,
      Granularity segmentGranularity,
      int numSegmentsPerTimePartition,
      int numRowsPerSegment,
      double nullRatio,
      // custom aggregators for indexing. can be used for testing rollup
      @Nullable List<AggregatorFactory> aggregatorFactories,
      boolean rollup,
      boolean persist
  )
  {
    this.rollup = rollup;
    this.persist = persist;
    this.numTimePartitions = Iterables.size(segmentGranularity.getIterable(interval));
    this.numRowsPerTimePartition = numSegmentsPerTimePartition;

    final int numRows = numRowsPerSegment * numRowsPerTimePartition * numTimePartitions;
    final List<TestColumnSchema> columnSchemas = ImmutableList.of(
        TestColumnSchema.makeSequential(
            TestColumn.FLOAT_COLUMN.getName(),
            TestColumn.FLOAT_COLUMN.getValueType(),
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeSequential(
            TestColumn.DOUBLE_COLUMN.getName(),
            TestColumn.DOUBLE_COLUMN.getValueType(),
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeSequential(
            TestColumn.LONG_COLUMN.getName(),
            TestColumn.LONG_COLUMN.getValueType(),
            false,
            1,
            nullRatio,
            -(numRows / 2),
            numRows / 2
        ),
        TestColumnSchema.makeEnumeratedSequential(
            TestColumn.SINGLE_VALUED_STRING_COLUMN.getName(),
            TestColumn.SINGLE_VALUED_STRING_COLUMN.getValueType(),
            false,
            1,
            nullRatio,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null)
        ),
        TestColumnSchema.makeEnumeratedSequential(
            TestColumn.MULTI_VALUED_STRING_COLUMN.getName(),
            TestColumn.MULTI_VALUED_STRING_COLUMN.getValueType(),
            false,
            4,
            nullRatio,
            Arrays.asList("Apple", "Orange", "Xylophone", "Corundum", null)
        )
    );

    this.testSchemaInfo = new TestSchemaInfo(
        columnSchemas,
        aggregatorFactories == null ? Collections.emptyList() : aggregatorFactories,
        interval,
        rollup
    );
    this.dataGenerator = new TestDataGenerator(columnSchemas, 0, interval, numRowsPerTimePartition);
  }

  @Before
  public void setupTestBase()
  {
    if (persist) {
      indexHolder = new QueryableIndexHolder(
          new ProcessingTestToolbox(),
          dataGenerator,
          testSchemaInfo,
          numTimePartitions,
          numRowsPerTimePartition,
          rollup,
          () -> {
            try {
              return temporaryFolder.newFolder();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    } else {
      indexHolder = new IncremenalIndexHolder(
          dataGenerator,
          testSchemaInfo,
          numTimePartitions,
          numRowsPerTimePartition,
          rollup
      );
    }
  }

  @After
  public void tearDownTestBase() throws IOException
  {
    if (indexHolder != null) {
      indexHolder.close();
    }
  }

  /**
   * A method to compute an expected result of {@link #aggregate} or {@link #bufferAggregate}.
   * The {@code valueExtractFunction} or {@code accumulateFunction} should handle nulls properly.
   */
  public <T, R> R compute(
      TestColumn column,
      Interval interval,
      Function<T, R> valueExtractFunction,
      BiFunction<R, R, R> accumulateFunction,
      @Nullable R initialValue
  )
  {
    return createCursorSequence(interval)
        .map(cursor -> {
          ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
          ColumnValueSelector<T> columnValueSelector = columnSelectorFactory.makeColumnValueSelector(column.getName());
          R accumulated = initialValue;
          while (!cursor.isDone()) {
            T val = columnValueSelector.getObject();
            accumulated = accumulateFunction.apply(accumulated, valueExtractFunction.apply(val));
            cursor.advance();
          }
          return accumulated;
        })
        .accumulate(initialValue, accumulateFunction::apply);
  }

  public <T> T aggregate(AggregatorFactory aggregatorFactory, Interval interval)
  {
    return createCursorSequence(interval)
        .map(cursor -> {
          Aggregator aggregator = aggregatorFactory.factorize(cursor.getColumnSelectorFactory());
          while (!cursor.isDone()) {
            aggregator.aggregate();
            cursor.advance();
          }
          aggregator.close();
          return aggregator.get();
        })
        .accumulate(null, (accumulated, val) -> (T) aggregatorFactory.combine(accumulated, val));
  }

  public <T> T bufferAggregate(AggregatorFactory aggregatorFactory, Interval interval)
  {
    ByteBuffer buffer = ByteBuffer.allocate(
        isReplaceNullWithDefault()
        ? aggregatorFactory.getMaxIntermediateSize()
        : aggregatorFactory.getMaxIntermediateSizeWithNulls()
    );
    return createCursorSequence(interval)
        .map(cursor -> {
          BufferAggregator aggregator = aggregatorFactory.factorizeBuffered(cursor.getColumnSelectorFactory());
          aggregator.init(buffer, 0);
          while (!cursor.isDone()) {
            aggregator.aggregate(buffer, 0);
            cursor.advance();
          }
          aggregator.close();
          return aggregator.get(buffer, 0);
        })
        .accumulate(null, (accumulated, val) -> (T) aggregatorFactory.combine(accumulated, val));
  }

  public Sequence<Cursor> createCursorSequence(Interval interval)
  {
    return indexHolder.createCursorSequence(interval);
  }

  /**
   * Creates indexes, creates a cursor sequence from underlying indexes, and cleans up the indexes after testing.
   */
  private interface IndexHolder extends Closeable
  {
    Sequence<Cursor> createCursorSequence(Interval interval);
  }

  private static class IncremenalIndexHolder implements IndexHolder
  {
    private final List<IncrementalIndex<?>> indexList = new ArrayList<>();

    private IncremenalIndexHolder(
        TestDataGenerator dataGenerator,
        TestSchemaInfo testSchemaInfo,
        int numTimePartitions,
        int numRowsPerTimePartition,
        boolean rollup
    )
    {
      createIncrementalIndexes(
          dataGenerator,
          testSchemaInfo,
          numTimePartitions,
          numRowsPerTimePartition,
          rollup,
          indexList::add
      );
    }

    @Override
    public Sequence<Cursor> createCursorSequence(Interval interval)
    {
      return Sequences
          .simple(indexList)
          .flatMap(index -> new IncrementalIndexStorageAdapter(index).makeCursors(
              null,
              interval,
              VirtualColumns.EMPTY,
              Granularities.ALL,
              false,
              null
          ));
    }

    @Override
    public void close() throws IOException
    {
      final Closer closer = Closer.create();
      closer.registerAll(indexList);
      closer.close();
    }
  }

  private static class QueryableIndexHolder implements IndexHolder
  {
    private final List<QueryableIndex> indexList = new ArrayList<>();

    private QueryableIndexHolder(
        ProcessingTestToolbox toolbox,
        TestDataGenerator dataGenerator,
        TestSchemaInfo testSchemaInfo,
        int numTimePartitions,
        int numRowsPerTimePartition,
        boolean rollup,
        Supplier<File> tmpDirSupplier
    )
    {
      final File tmpDir = tmpDirSupplier.get();
      createIncrementalIndexes(
          dataGenerator,
          testSchemaInfo, numTimePartitions,
          numRowsPerTimePartition,
          rollup,
          incrementalIndex -> {
            try {
              final File file = toolbox.getIndexMerger().persist(
                  incrementalIndex,
                  new File(tmpDir, UUIDUtils.generateUuid()),
                  new IndexSpec(),
                  null
              );
              indexList.add(toolbox.getIndexIO().loadIndex(file));
              incrementalIndex.close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }

    @Override
    public Sequence<Cursor> createCursorSequence(Interval interval)
    {
      return Sequences
          .simple(indexList)
          .flatMap(index -> new QueryableIndexStorageAdapter(index).makeCursors(
              null,
              interval,
              VirtualColumns.EMPTY,
              Granularities.ALL,
              false,
              null
          ));
    }

    @Override
    public void close() throws IOException
    {
      final Closer closer = Closer.create();
      closer.registerAll(indexList);
      closer.close();
    }
  }

  private static void createIncrementalIndexes(
      TestDataGenerator dataGenerator,
      TestSchemaInfo testSchemaInfo,
      int numTimePartitions,
      int numRowsPerTimePartition,
      boolean rollup,
      Consumer<IncrementalIndex<?>> consumer
  )
  {
    TestColumn[] columns = TestColumn.values();
    Boolean[] includeNulls = new Boolean[columns.length];
    Arrays.fill(includeNulls, false);
    for (int i = 0; i < numTimePartitions; i++) {
      IncrementalIndex<Aggregator> incrementalIndex = new IncrementalIndex.Builder()
          .setIndexSchema(
              new IncrementalIndexSchema.Builder()
                  .withDimensionsSpec(testSchemaInfo.getDimensionsSpec())
                  .withMetrics(testSchemaInfo.getAggsArray())
                  .withRollup(rollup)
                  .build()
          )
          .setMaxRowCount(numRowsPerTimePartition) // create one segment per time chunk for the sake of convenience
          .buildOnheap();

      try {
        for (int j = 0; j < numRowsPerTimePartition; j++) {
          InputRow inputRow = dataGenerator.nextRow();
          for (int k = 0; k < columns.length; k++) {
            includeNulls[k] |= inputRow.getRaw(columns[k].getName()) == null;
          }
          incrementalIndex.add(inputRow);
        }
        consumer.accept(incrementalIndex);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (!Arrays.stream(includeNulls).allMatch(includeNull -> includeNull)) {
      throw new ISE(
          "Found columns with no null value. It may be because of too small number of rows to create. "
          + "Try increasing numRows or nullRatio."
      );
    }
  }

  static class SettableColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final ColumnValueSelector columnValueSelector;

    SettableColumnSelectorFactory(ColumnValueSelector columnValueSelector)
    {
      this.columnValueSelector = columnValueSelector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      return columnValueSelector;
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }
}
