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

package io.druid.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.VSizeIndexedInts;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class IndexedIntsWrappingBenchmark
{
  private class IndexedIntsDimensionSelectorRow implements Iterable<Comparable>
  {
    private final IndexedInts delegate;

    public IndexedIntsDimensionSelectorRow(IndexedInts delegate)
    {
      this.delegate = delegate;
    }

    public Iterator iterator()
    {
      return delegate.iterator();
    }

    public int size()
    {
      return delegate.size();
    }

    public Comparable get(int index)
    {
      return delegate.get(index);
    }

    public IndexedInts getDelegate()
    {
      return delegate;
    }
  }

  static final int ROW_SIZE = 64;
  static final int MAX_ROWS = 250000;
  static final int MAX_VALUE = 2000000000;
  static final boolean USE_VSIZE = false;

  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private IncrementalIndex incIndex;
  private QueryableIndex qIndex;

  private static AggregatorFactory[] AGGS;

  private ArrayList<IndexedInts> rows = new ArrayList<>();
  private ArrayList<IndexedIntsDimensionSelectorRow> wrappedRows = new ArrayList<>();
  private ArrayList<DimensionSelector> incSelectors = new ArrayList<>();
  private ArrayList<DimensionSelector> qSelectors = new ArrayList<>();
  private Cursor incCursor;
  private Cursor qCursor;

  private Random rng;
  private Random rng2;
  private File tmpFile;

  private StorageAdapter incAdapter;
  private StorageAdapter qAdapter;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  static {
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>(ROW_SIZE + 1);
    ingestAggregatorFactories.add(new CountAggregatorFactory("rows"));
    for (int i = 0; i < ROW_SIZE; ++i) {
      if (i % 2 != 0) {
        continue;
      }

      ingestAggregatorFactories.add(
          new LongSumAggregatorFactory(
              String.format("sumResult%s", i),
              String.format("Dim_%s", i)
          )
      );
      ingestAggregatorFactories.add(
          new DoubleSumAggregatorFactory(
              String.format("doubleSumResult%s", i),
              String.format("Dim_%s", i)
          )
      );
    }
    AGGS = ingestAggregatorFactories.toArray(new AggregatorFactory[0]);
  }

  @Setup
  public void setupTrial() throws IOException
  {
    rng = new Random(9999);
    rng2 = new Random(9999);

    rows = new ArrayList<>();
    wrappedRows = new ArrayList<>();
    for (int i = 0; i < MAX_ROWS; i++) {
      IndexedInts row = getIndexedInts(USE_VSIZE);
      IndexedIntsDimensionSelectorRow wrappedRow = wrapIndexedInts(row);
      rows.add(row);
      wrappedRows.add(wrappedRow);
    }

    if (incIndex != null) {
      incIndex.close();
    }
    incIndex = makeIncIndex();
    incAdapter = new IncrementalIndexStorageAdapter(incIndex);
  }

  @Setup(Level.Iteration)
  public void setupIteration() throws IOException
  {
    Sequence<Cursor> incCursors = incAdapter.makeCursors(null, new Interval(0L, 10L), QueryGranularity.NONE, false);
    incCursor = Sequences.toList(Sequences.limit(incCursors, 1), Lists.<Cursor>newArrayList()).get(0);
    incCursor.reset();
    incSelectors = new ArrayList<>();
    for (int i = 0; i < ROW_SIZE; i++) {
      String dimName = String.format("Dim_%s", i);
      incSelectors.add(incCursor.makeDimensionSelector(new DefaultDimensionSpec(dimName, dimName)));
    }
  }

  private IndexedInts getIndexedInts(boolean useVSize)
  {
    final int[] values = new int[ROW_SIZE];
    for(int i = 0; i < values.length; i++) {
      values[i] = rng.nextInt(MAX_VALUE);
    }

    if(useVSize) {
      return VSizeIndexedInts.fromArray(values);
    } else {
      IndexedInts retVal = new IndexedInts()
      {
        @Override
        public int size()
        {
          return values.length;
        }

        @Override
        public int get(int index)
        {
          return values[index];
        }

        @Override
        public Iterator<Integer> iterator()
        {
          return Ints.asList(values).iterator();
        }

        @Override
        public void fill(int index, int[] toFill)
        {
          throw new UnsupportedOperationException("fill not supported");
        }

        @Override
        public void close() throws IOException
        {

        }
      };
      return retVal;
    }
  }

  private IndexedIntsDimensionSelectorRow wrapIndexedInts(IndexedInts values)
  {
    return new IndexedIntsDimensionSelectorRow(values);
  }

  private IncrementalIndex makeIncIndex() throws IOException
  {
    IncrementalIndex index = new OnheapIncrementalIndex(
        0,
        QueryGranularity.NONE,
        AGGS,
        false,
        true,
        true,
        MAX_ROWS
    );

    for (int i = 0; i < MAX_ROWS; i++) {
      InputRow row = getStringRow(5L, i, ROW_SIZE);
      index.add(row);
    }

    return index;
  }

  private MapBasedInputRow getStringRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, String.valueOf(rng2.nextInt(1000)));
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void unwrapped(Blackhole blackhole) throws Exception
  {
    int sum = 0;
    for (int i = 0; i < MAX_ROWS; i++) {
      IndexedInts row = rows.get(i);
      for (int val : row) {
        sum += val;
        blackhole.consume(val);
      }
    }
    blackhole.consume(sum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void unwrappedCast(Blackhole blackhole) throws Exception
  {
    int sum = 0;
    for (int i = 0; i < MAX_ROWS; i++) {
      IndexedInts row = rows.get(i);
      for (int val : row) {
        sum += (Integer) val;
        blackhole.consume((Integer) val);
      }
    }
    blackhole.consume(sum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void unwrappedCast2(Blackhole blackhole) throws Exception
  {
    int sum = 0;
    for (int i = 0; i < MAX_ROWS; i++) {
      IndexedInts row = rows.get(i);
      for (int val : row) {
        Integer iVal = (Integer) val;
        sum += iVal;
        blackhole.consume(iVal);
      }
    }
    blackhole.consume(sum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void wrapped(Blackhole blackhole) throws Exception
  {
    int sum = 0;
    for (int i = 0; i < MAX_ROWS; i++) {
      IndexedInts row = rows.get(i);
      IndexedIntsDimensionSelectorRow wrow = wrapIndexedInts(row);
      for (Comparable val : wrow) {
        sum += (Integer) val;
        blackhole.consume(val);
      }
    }
    blackhole.consume(sum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void readIncIndex(Blackhole blackhole) throws Exception
  {
    int sum = 0;
    for (int i = 0; i < MAX_ROWS; i++) {
      for (int j = 0; j < ROW_SIZE; j++) {
        IndexedInts row = incSelectors.get(j).getRow();
        for (int val : row) {
          sum += val;
          blackhole.consume(val);
        }
      }
      incCursor.advance();
    }
    blackhole.consume(sum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @OperationsPerInvocation(MAX_ROWS)
  public void readIncIndexCast(Blackhole blackhole) throws Exception
  {
    int sum = 0;
    for (int i = 0; i < MAX_ROWS; i++) {
      for (int j = 0; j < ROW_SIZE; j++) {
        IndexedInts row = incSelectors.get(j).getRow();
        IndexedIntsDimensionSelectorRow dsRow = new IndexedIntsDimensionSelectorRow(row);
        for (Comparable val : dsRow) {
          sum += (Integer) val;
          blackhole.consume(val);
        }
      }
      incCursor.advance();
    }
    blackhole.consume(sum);
  }
}
