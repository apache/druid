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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class TestIndex
{
  public static final String[] COLUMNS = new String[]{
      "ts",
      "market",
      "quality",
      "qualityLong",
      "qualityFloat",
      "qualityDouble",
      "qualityNumericString",
      "placement",
      "placementish",
      "index",
      "partial_null_column",
      "null_column",
      "quality_uniques",
      "indexMin",
      "indexMaxPlusTen"
  };
  public static final String[] DIMENSIONS = new String[]{
      "market",
      "quality",
      "qualityLong",
      "qualityFloat",
      "qualityDouble",
      "qualityNumericString",
      "placement",
      "placementish",
      "partial_null_column",
      "null_column"
  };

  public static final List<DimensionSchema> DIMENSION_SCHEMAS = Arrays.asList(
      new StringDimensionSchema("market"),
      new StringDimensionSchema("quality"),
      new LongDimensionSchema("qualityLong"),
      new FloatDimensionSchema("qualityFloat"),
      new DoubleDimensionSchema("qualityDouble"),
      new StringDimensionSchema("qualityNumericString"),
      new StringDimensionSchema("placement"),
      new StringDimensionSchema("placementish"),
      new StringDimensionSchema("partial_null_column"),
      new StringDimensionSchema("null_column")
  );

  public static final List<DimensionSchema> DIMENSION_SCHEMAS_NO_BITMAP = Arrays.asList(
      new StringDimensionSchema("market", null, false),
      new StringDimensionSchema("quality", null, false),
      new LongDimensionSchema("qualityLong"),
      new FloatDimensionSchema("qualityFloat"),
      new DoubleDimensionSchema("qualityDouble"),
      new StringDimensionSchema("qualityNumericString", null, false),
      new StringDimensionSchema("placement", null, false),
      new StringDimensionSchema("placementish", null, false),
      new StringDimensionSchema("partial_null_column", null, false),
      new StringDimensionSchema("null_column", null, false)
  );

  public static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DIMENSION_SCHEMAS,
      null,
      null
  );

  public static final DimensionsSpec DIMENSIONS_SPEC_NO_BITMAPS = new DimensionsSpec(
      DIMENSION_SCHEMAS_NO_BITMAP,
      null,
      null
  );

  public static final String[] DOUBLE_METRICS = new String[]{"index", "indexMin", "indexMaxPlusTen"};
  public static final String[] FLOAT_METRICS = new String[]{"indexFloat", "indexMinFloat", "indexMaxFloat"};
  private static final Logger log = new Logger(TestIndex.class);
  private static final Interval DATA_INTERVAL = Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  private static final VirtualColumns VIRTUAL_COLUMNS = VirtualColumns.create(
      Collections.singletonList(
          new ExpressionVirtualColumn("expr", "index + 10", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
      )
  );
  public static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory(DOUBLE_METRICS[0], "index"),
      new FloatSumAggregatorFactory(FLOAT_METRICS[0], "index"),
      new DoubleMinAggregatorFactory(DOUBLE_METRICS[1], "index"),
      new FloatMinAggregatorFactory(FLOAT_METRICS[1], "index"),
      new FloatMaxAggregatorFactory(FLOAT_METRICS[2], "index"),
      new DoubleMaxAggregatorFactory(DOUBLE_METRICS[2], VIRTUAL_COLUMNS.getVirtualColumns()[0].getOutputName()),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };
  private static final IndexSpec INDEX_SPEC = new IndexSpec();

  private static final IndexMerger INDEX_MERGER =
      TestHelper.getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance());
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  static {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());
  }

  private static Supplier<IncrementalIndex> realtimeIndex = Suppliers.memoize(
      () -> makeRealtimeIndex("druid.sample.numeric.tsv")
  );
  private static Supplier<IncrementalIndex> noRollupRealtimeIndex = Suppliers.memoize(
      () -> makeRealtimeIndex("druid.sample.numeric.tsv", false)
  );
  private static Supplier<IncrementalIndex> noBitmapRealtimeIndex = Suppliers.memoize(
      () -> makeRealtimeIndex("druid.sample.numeric.tsv", false, false)
  );
  private static Supplier<QueryableIndex> mmappedIndex = Suppliers.memoize(
      () -> persistRealtimeAndLoadMMapped(realtimeIndex.get())
  );
  private static Supplier<QueryableIndex> noRollupMmappedIndex = Suppliers.memoize(
      () -> persistRealtimeAndLoadMMapped(noRollupRealtimeIndex.get())
  );
  private static Supplier<QueryableIndex> noBitmapMmappedIndex = Suppliers.memoize(
      () -> persistRealtimeAndLoadMMapped(noBitmapRealtimeIndex.get())
  );
  private static Supplier<QueryableIndex> mergedRealtime = Suppliers.memoize(() -> {
    try {
      IncrementalIndex top = makeRealtimeIndex("druid.sample.numeric.tsv.top");
      IncrementalIndex bottom = makeRealtimeIndex("druid.sample.numeric.tsv.bottom");

      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();

      File topFile = new File(tmpFile, "top");
      File bottomFile = new File(tmpFile, "bottom");
      File mergedFile = new File(tmpFile, "merged");

      topFile.mkdirs();
      topFile.deleteOnExit();
      bottomFile.mkdirs();
      bottomFile.deleteOnExit();
      mergedFile.mkdirs();
      mergedFile.deleteOnExit();

      INDEX_MERGER.persist(top, DATA_INTERVAL, topFile, INDEX_SPEC, null);
      INDEX_MERGER.persist(bottom, DATA_INTERVAL, bottomFile, INDEX_SPEC, null);

      return INDEX_IO.loadIndex(
          INDEX_MERGER.mergeQueryableIndex(
              Arrays.asList(INDEX_IO.loadIndex(topFile), INDEX_IO.loadIndex(bottomFile)),
              true,
              METRIC_AGGS,
              mergedFile,
              INDEX_SPEC,
              null
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  });

  public static IncrementalIndex getIncrementalTestIndex()
  {
    return realtimeIndex.get();
  }

  public static IncrementalIndex getNoRollupIncrementalTestIndex()
  {
    return noRollupRealtimeIndex.get();
  }

  public static IncrementalIndex getNoBitmapIncrementalTestIndex()
  {
    return noBitmapRealtimeIndex.get();
  }

  public static QueryableIndex getMMappedTestIndex()
  {
    return mmappedIndex.get();
  }

  public static QueryableIndex getNoRollupMMappedTestIndex()
  {
    return noRollupMmappedIndex.get();
  }

  public static QueryableIndex getNoBitmapMMappedTestIndex()
  {
    return noBitmapMmappedIndex.get();
  }

  public static QueryableIndex mergedRealtimeIndex()
  {
    return mergedRealtime.get();
  }

  public static IncrementalIndex makeRealtimeIndex(final String resourceFilename)
  {
    return makeRealtimeIndex(resourceFilename, true);
  }

  public static IncrementalIndex makeRealtimeIndex(final String resourceFilename, boolean rollup)
  {
    return makeRealtimeIndex(resourceFilename, rollup, true);
  }

  public static IncrementalIndex makeRealtimeIndex(final String resourceFilename, boolean rollup, boolean bitmap)
  {
    final URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceFilename);
    }
    log.info("Realtime loading index file[%s]", resource);
    CharSource stream = Resources.asByteSource(resource).asCharSource(StandardCharsets.UTF_8);
    return makeRealtimeIndex(stream, rollup, bitmap);
  }

  public static IncrementalIndex makeRealtimeIndex(final CharSource source)
  {
    return makeRealtimeIndex(source, true, true);
  }

  public static IncrementalIndex makeRealtimeIndex(final CharSource source, boolean rollup, boolean bitmap)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
        .withTimestampSpec(new TimestampSpec("ds", "auto", null))
        .withDimensionsSpec(bitmap ? DIMENSIONS_SPEC : DIMENSIONS_SPEC_NO_BITMAPS)
        .withVirtualColumns(VIRTUAL_COLUMNS)
        .withMetrics(METRIC_AGGS)
        .withRollup(rollup)
        .build();
    final IncrementalIndex retVal = new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10000)
        .buildOnheap();

    try {
      return loadIncrementalIndex(retVal, source);
    }
    catch (Exception e) {
      if (rollup) {
        realtimeIndex = null;
      } else {
        noRollupRealtimeIndex = null;
      }
      throw new RuntimeException(e);
    }
  }

  public static IncrementalIndex loadIncrementalIndex(
      final IncrementalIndex retVal,
      final CharSource source
  ) throws IOException
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("ts", "iso", null),
            new DimensionsSpec(DIMENSION_SCHEMAS, null, null),
            "\t",
            "\u0001",
            Arrays.asList(COLUMNS),
            false,
            0
        ),
        "utf8"
    );
    return loadIncrementalIndex(() -> retVal, source, parser);
  }

  public static IncrementalIndex loadIncrementalIndex(
      final Supplier<IncrementalIndex> indexSupplier,
      final CharSource source,
      final StringInputRowParser parser
  ) throws IOException
  {
    final IncrementalIndex retVal = indexSupplier.get();
    final AtomicLong startTime = new AtomicLong();
    int lineCount = source.readLines(
        new LineProcessor<Integer>()
        {
          boolean runOnce = false;
          int lineCount = 0;

          @Override
          public boolean processLine(String line) throws IOException
          {
            if (!runOnce) {
              startTime.set(System.currentTimeMillis());
              runOnce = true;
            }
            retVal.add(parser.parse(line));

            ++lineCount;
            return true;
          }

          @Override
          public Integer getResult()
          {
            return lineCount;
          }
        }
    );

    log.info("Loaded %,d lines in %,d millis.", lineCount, System.currentTimeMillis() - startTime.get());

    return retVal;
  }

  public static QueryableIndex persistRealtimeAndLoadMMapped(IncrementalIndex index)
  {
    try {
      File someTmpFile = File.createTempFile("billy", "yay");
      someTmpFile.delete();
      someTmpFile.mkdirs();
      someTmpFile.deleteOnExit();

      INDEX_MERGER.persist(index, someTmpFile, INDEX_SPEC, null);
      return INDEX_IO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
