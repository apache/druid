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
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.ExtractionFilter;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ColumnScanBenchmark
{
  static final int TIMESTAMP_DUPLICATE_COUNT = 100;
  static final int MAX_ROWS = 250000;
  static final String SECOND_TIME_NAME = "time2";

  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private QueryableIndex qIndex;

  private static AggregatorFactory[] AGGS;

  private Random rng;
  private Random rng2;
  private File tmpFile;

  private StorageAdapter qAdapter;

  private boolean initialized = false;

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
    final ArrayList<AggregatorFactory> ingestAggregatorFactories = new ArrayList<>();
    AGGS = ingestAggregatorFactories.toArray(new AggregatorFactory[0]);
  }

  @Setup
  public void setup() throws IOException
  {
    if(initialized) {
      return;
    }

    rng = new Random(9999);
    rng2 = new Random(9999);

    IncrementalIndex index = makeIncIndex();
    File tmpFile = File.createTempFile("IndexedIntsWrappingBenchmark-INDEX-", String.valueOf(rng.nextInt(10)));
    tmpFile.delete();
    tmpFile.mkdirs();
    System.out.println(tmpFile.getAbsolutePath() + " isFile: " + tmpFile.isFile() + " isDir:" + tmpFile.isDirectory());
    tmpFile.deleteOnExit();
    File indexFile = INDEX_MERGER_V9.persist(
        index,
        tmpFile,
        new IndexSpec()
    );
    index.close();
    index = null;

    //File theIndex = new File("/Users/user/bench_index/64dim_card20000");

    if (qIndex != null) {
      qIndex.close();
    }
    qIndex = makeQueryableIndexFromFile(indexFile);
    qAdapter = new QueryableIndexStorageAdapter(qIndex);
    initialized = true;
  }

  private IncrementalIndex makeIncIndex() throws IOException
  {
    IncrementalIndex index = new OnheapIncrementalIndex(
        0,
        QueryGranularity.NONE,
        AGGS,
        MAX_ROWS
    );

    int row_id = 0;
    int ts_count = 0;
    long timestamp = 1000L;
    for (int i = 0; i < MAX_ROWS; i++) {
      if (ts_count >= TIMESTAMP_DUPLICATE_COUNT) {
        ts_count = 0;
        timestamp += 3600001L; // jump an hour
      } else {
        ts_count += 1;
      }
      row_id += 1;
      InputRow row = getStringRow(timestamp, row_id);
      index.add(row);
    }

    return index;
  }

  private QueryableIndex makeQueryableIndex(IncrementalIndex incIndex) throws IOException
  {
    QueryableIndex index =
        INDEX_IO.loadIndex(
            INDEX_MERGER_V9.persist(
                incIndex,
                tmpFile,
                new IndexSpec()
            )
        );
    return index;
  }

  private QueryableIndex makeQueryableIndexFromFile(File file) throws IOException
  {
    QueryableIndex index =
        INDEX_IO.loadIndex(file);
    return index;
  }

  private MapBasedInputRow getStringRow(long timestamp, int row_id)
  {
    List<String> dimensionList = new ArrayList<>();
    dimensionList.add(SECOND_TIME_NAME);
    dimensionList.add("row_id");

    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put(SECOND_TIME_NAME, new DateTime(timestamp, DateTimeZone.UTC).toString());
    builder.put("row_id", row_id);
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void loadMainTime(Blackhole blackhole) throws Exception
  {
    ExtractionFn extfn = new TimeFormatExtractionFn("H", DateTimeZone.UTC, null);
    ExtractionFilter hrFilter = new ExtractionFilter(
        Column.TIME_COLUMN_NAME,
        "8",
        extfn
    );

    int sum = 0;
    Sequence<Cursor> qCursors = qAdapter.makeCursors(
        hrFilter,
        new Interval(0L, JodaUtils.MAX_INSTANT),
        QueryGranularity.ALL,
        false
    );

    Cursor qCursor = Sequences.toList(Sequences.limit(qCursors, 1), Lists.<Cursor>newArrayList()).get(0);

    DimensionSelector selector = qCursor.makeDimensionSelector(new ExtractionDimensionSpec(
        Column.TIME_COLUMN_NAME,
        Column.TIME_COLUMN_NAME,
        extfn
    ));

    DimensionSelector rowNumSelector = qCursor.makeDimensionSelector(new DefaultDimensionSpec("row_id", "row_id"));


    for (int i = 0; i < MAX_ROWS; i++) {
      if (qCursor.isDone()) {
        System.out.println("CURSOR DONE, i=" + i);
        break;
      }
      IndexedInts row = selector.getRow();
      for (int val : row) {
        sum += val;
        blackhole.consume(val);
      }
      IndexedInts row2 = rowNumSelector.getRow();
      for (int val : row2) {
        sum += val;
        blackhole.consume(val);
      }

      qCursor.advance();
    }
    blackhole.consume(sum);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void loadSecondTime(Blackhole blackhole) throws Exception
  {
    ExtractionFn extfn = new TimeFormatExtractionFn("H", DateTimeZone.UTC, null);
    ExtractionFilter hrFilter = new ExtractionFilter(
        SECOND_TIME_NAME,
        "8",
        extfn
    );

    int sum = 0;
    Sequence<Cursor> qCursors = qAdapter.makeCursors(
        hrFilter,
        new Interval(0L, JodaUtils.MAX_INSTANT),
        QueryGranularity.ALL,
        false
    );
    Cursor qCursor = Sequences.toList(Sequences.limit(qCursors, 1), Lists.<Cursor>newArrayList()).get(0);

    DimensionSelector selector = qCursor.makeDimensionSelector(new ExtractionDimensionSpec(
        SECOND_TIME_NAME,
        SECOND_TIME_NAME,
        extfn
    ));

    DimensionSelector rowNumSelector = qCursor.makeDimensionSelector(new DefaultDimensionSpec("row_id", "row_id"));


    for (int i = 0; i < MAX_ROWS; i++) {
      if (qCursor.isDone()) {
        System.out.println("CURSOR DONE, i=" + i);
        break;
      }

      IndexedInts row = selector.getRow();
      for (int val : row) {
        sum += val;
        blackhole.consume(val);
      }

      IndexedInts row2 = rowNumSelector.getRow();
      for (int val : row2) {
        sum += val;
        blackhole.consume(val);
      }

      qCursor.advance();
    }
    blackhole.consume(sum);
  }
}
