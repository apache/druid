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
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.FrontCodedIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
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
import java.util.stream.Collectors;

/**
 *
 */
public class TestIndex
{
  public static final String SAMPLE_NUMERIC_JSON = "druid.sample.numeric.json";
  public static final String SAMPLE_NUMERIC_JSON_TOP = "druid.sample.numeric.top.json";
  public static final String SAMPLE_NUMERIC_JSON_BOTTOM = "druid.sample.numeric.bottom.json";

  public static final String[] COLUMNS = new String[]{
      "ts",
      "market",
      "quality",
      "qualityLong",
      "qualityFloat",
      "qualityDouble",
      "qualityNumericString",
      "longNumericNull",
      "floatNumericNull",
      "doubleNumericNull",
      "placement",
      "placementish",
      "index",
      "partial_null_column",
      "null_column",
      "quality_uniques",
      "indexMin",
      "indexMaxPlusTen"
  };

  public static final List<DimensionSchema> DIMENSION_SCHEMAS = Arrays.asList(
      new StringDimensionSchema("market"),
      new StringDimensionSchema("quality"),
      new LongDimensionSchema("qualityLong"),
      new FloatDimensionSchema("qualityFloat"),
      new DoubleDimensionSchema("qualityDouble"),
      new StringDimensionSchema("qualityNumericString"),
      new LongDimensionSchema("longNumericNull"),
      new FloatDimensionSchema("floatNumericNull"),
      new DoubleDimensionSchema("doubleNumericNull"),
      new StringDimensionSchema("placement"),
      new StringDimensionSchema("placementish"),
      new StringDimensionSchema("null_column")
  );

  public static final List<DimensionSchema> DIMENSION_SCHEMAS_NON_TIME_ORDERED = Arrays.asList(
      new StringDimensionSchema("market"),
      new StringDimensionSchema("quality"),
      new LongDimensionSchema("__time"),
      new LongDimensionSchema("qualityLong"),
      new FloatDimensionSchema("qualityFloat"),
      new DoubleDimensionSchema("qualityDouble"),
      new StringDimensionSchema("qualityNumericString"),
      new LongDimensionSchema("longNumericNull"),
      new FloatDimensionSchema("floatNumericNull"),
      new DoubleDimensionSchema("doubleNumericNull"),
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
      new LongDimensionSchema("longNumericNull"),
      new FloatDimensionSchema("floatNumericNull"),
      new DoubleDimensionSchema("doubleNumericNull"),
      new StringDimensionSchema("placement", null, false),
      new StringDimensionSchema("placementish", null, false),
      new StringDimensionSchema("partial_null_column", null, false),
      new StringDimensionSchema("null_column", null, false)
  );

  public static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(DIMENSION_SCHEMAS);
  public static final DimensionsSpec DIMENSIONS_SPEC_PARTIAL_NO_STRINGS =
      new DimensionsSpec(
          DIMENSION_SCHEMAS.stream().filter(x -> !(x instanceof StringDimensionSchema)).collect(Collectors.toList())
      );
  public static final DimensionsSpec DIMENSIONS_SPEC_NON_TIME_ORDERED =
      new DimensionsSpec(DIMENSION_SCHEMAS_NON_TIME_ORDERED);
  public static final DimensionsSpec DIMENSIONS_SPEC_NO_BITMAPS = new DimensionsSpec(DIMENSION_SCHEMAS_NO_BITMAP);

  public static final String[] DOUBLE_METRICS = new String[]{"index", "indexMin", "indexMaxPlusTen"};
  public static final String[] FLOAT_METRICS = new String[]{"indexFloat", "indexMinFloat", "indexMaxFloat"};
  public static final Interval DATA_INTERVAL = Intervals.of("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  private static final Logger log = new Logger(TestIndex.class);
  private static final VirtualColumns VIRTUAL_COLUMNS = VirtualColumns.create(
      Collections.singletonList(
          new ExpressionVirtualColumn("expr", "index + 10", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
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
  public static final ImmutableList<AggregateProjectionSpec> PROJECTIONS = ImmutableList.of(
      new AggregateProjectionSpec(
          "index_projection",
          VirtualColumns.create(Granularities.toVirtualColumn(Granularities.DAY, "__gran")),
          Arrays.asList(
              new LongDimensionSchema("__gran"),
              new StringDimensionSchema("market")
          ),
          new AggregatorFactory[]{new DoubleMaxAggregatorFactory("maxQuality", "qualityLong")}
      )
  );
  public static final IndexSpec INDEX_SPEC = IndexSpec.DEFAULT;

  public static final JsonInputFormat DEFAULT_JSON_INPUT_FORMAT = new JsonInputFormat(
      JSONPathSpec.DEFAULT,
      null,
      null,
      null,
      null
  );

  public static final IndexMerger INDEX_MERGER =
      TestHelper.getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance());
  public static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  static {
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());
  }

  private static Supplier<IncrementalIndex> realtimeIndex = Suppliers.memoize(
      TestIndex::makeSampleNumericIncrementalIndex
  );
  private static Supplier<IncrementalIndex> realtimeIndexPartialSchemaLegacyStringDiscovery = Suppliers.memoize(
      () -> fromJsonResource(
          SAMPLE_NUMERIC_JSON,
          true,
          DIMENSIONS_SPEC_PARTIAL_NO_STRINGS
      )
  );
  private static Supplier<IncrementalIndex> nonTimeOrderedRealtimeIndex = Suppliers.memoize(
      () -> fromJsonResource(SAMPLE_NUMERIC_JSON, true, DIMENSIONS_SPEC_NON_TIME_ORDERED)
  );
  private static Supplier<IncrementalIndex> nonTimeOrderedNoRollupRealtimeIndex = Suppliers.memoize(
      () -> fromJsonResource(SAMPLE_NUMERIC_JSON, false, DIMENSIONS_SPEC_NON_TIME_ORDERED)
  );
  private static Supplier<IncrementalIndex> noRollupRealtimeIndex = Suppliers.memoize(
      () -> fromJsonResource(SAMPLE_NUMERIC_JSON, false, DIMENSIONS_SPEC)
  );
  private static Supplier<IncrementalIndex> noBitmapRealtimeIndex = Suppliers.memoize(
      () -> fromJsonResource(SAMPLE_NUMERIC_JSON, false, DIMENSIONS_SPEC_NO_BITMAPS)
  );
  private static Supplier<QueryableIndex> mmappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(makeSampleNumericIncrementalIndex())
  );

  private static Supplier<QueryableIndex> mmappedIndexCompressedComplex = Suppliers.memoize(
      () -> persistAndMemoryMap(
          makeSampleNumericIncrementalIndex(),
          IndexSpec.builder().withComplexMetricCompression(CompressionStrategy.LZ4).build()
      )
  );
  private static Supplier<QueryableIndex> nonTimeOrderedMmappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(fromJsonResource(SAMPLE_NUMERIC_JSON, true, DIMENSIONS_SPEC_NON_TIME_ORDERED))
  );
  private static Supplier<QueryableIndex> nonTimeOrderedNoRollupMmappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(fromJsonResource(SAMPLE_NUMERIC_JSON, false, DIMENSIONS_SPEC_NON_TIME_ORDERED))
  );
  private static Supplier<QueryableIndex> noRollupMmappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(fromJsonResource(SAMPLE_NUMERIC_JSON, false, DIMENSIONS_SPEC))
  );
  private static Supplier<QueryableIndex> noBitmapMmappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(fromJsonResource(SAMPLE_NUMERIC_JSON, false, DIMENSIONS_SPEC_NO_BITMAPS))
  );
  private static Supplier<QueryableIndex> mergedRealtime = Suppliers.memoize(() -> {
    try {
      IncrementalIndex top = makeSampleNumericTopIncrementalIndex();
      IncrementalIndex bottom = makeSampleNumericBottomIncrementalIndex();

      File tmpFile = File.createTempFile("yay", "who");
      tmpFile.delete();

      File topFile = new File(tmpFile, "top");
      File bottomFile = new File(tmpFile, "bottom");
      File mergedFile = new File(tmpFile, "merged");

      FileUtils.mkdirp(topFile);
      FileUtils.mkdirp(bottomFile);
      FileUtils.mkdirp(mergedFile);
      topFile.deleteOnExit();
      bottomFile.deleteOnExit();
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
              null,
              -1
          )
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  });
  private static Supplier<QueryableIndex> frontCodedMmappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(
          makeSampleNumericIncrementalIndex(),
          IndexSpec.builder()
                   .withStringDictionaryEncoding(
                       new StringEncodingStrategy.FrontCoded(4, FrontCodedIndexed.V1)
                   )
                   .build()
      )
  );
  private static Supplier<QueryableIndex> wikipediaMMappedIndex = Suppliers.memoize(
      () -> persistAndMemoryMap(makeWikipediaIncrementalIndex())
  );

  public static IncrementalIndex getIncrementalTestIndex()
  {
    return realtimeIndex.get();
  }

  public static IncrementalIndex getIncrementalTestIndexPartialSchemaLegacyStringDiscovery()
  {
    return realtimeIndexPartialSchemaLegacyStringDiscovery.get();
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

  public static QueryableIndex getMMappedWikipediaIndex()
  {
    return wikipediaMMappedIndex.get();
  }

  public static QueryableIndex getNoRollupMMappedTestIndex()
  {
    return noRollupMmappedIndex.get();
  }

  public static QueryableIndex getNoBitmapMMappedTestIndex()
  {
    return noBitmapMmappedIndex.get();
  }

  public static IncrementalIndex getNonTimeOrderedRealtimeTestIndex()
  {
    return nonTimeOrderedRealtimeIndex.get();
  }

  public static IncrementalIndex getNonTimeOrderedNoRollupRealtimeTestIndex()
  {
    return nonTimeOrderedNoRollupRealtimeIndex.get();
  }

  public static QueryableIndex getNonTimeOrderedMMappedTestIndex()
  {
    return nonTimeOrderedMmappedIndex.get();
  }

  public static QueryableIndex getNonTimeOrderedNoRollupMMappedTestIndex()
  {
    return nonTimeOrderedNoRollupMmappedIndex.get();
  }

  public static QueryableIndex mergedRealtimeIndex()
  {
    return mergedRealtime.get();
  }

  public static QueryableIndex getFrontCodedMMappedTestIndex()
  {
    return frontCodedMmappedIndex.get();
  }

  public static QueryableIndex getMMappedTestIndexCompressedComplex()
  {
    return mmappedIndexCompressedComplex.get();
  }

  public static IncrementalIndex makeSampleNumericIncrementalIndex()
  {
    return fromJsonResource(
        SAMPLE_NUMERIC_JSON,
        true,
        DIMENSIONS_SPEC
    );
  }

  public static IncrementalIndex makeSampleNumericTopIncrementalIndex()
  {
    return fromJsonResource(
        SAMPLE_NUMERIC_JSON_TOP,
        true,
        DIMENSIONS_SPEC
    );
  }

  public static IncrementalIndex makeSampleNumericBottomIncrementalIndex()
  {
    return fromJsonResource(
        SAMPLE_NUMERIC_JSON_BOTTOM,
        true,
        DIMENSIONS_SPEC
    );
  }

  private static IncrementalIndex fromJsonResource(
      final String resourceFilename,
      boolean rollup,
      DimensionsSpec dimensionsSpec
  )
  {
    return makeIncrementalIndexFromResource(
        resourceFilename,
        new IncrementalIndexSchema.Builder()
            .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
            .withTimestampSpec(new TimestampSpec("ts", "iso", null))
            .withDimensionsSpec(DimensionsSpec.builder()
                                              .setDimensions(dimensionsSpec.getDimensions())
                                              .setDimensionExclusions(ImmutableList.of("index"))
                                              .setIncludeAllDimensions(true)
                                              .build())
            .withVirtualColumns(VIRTUAL_COLUMNS)
            .withMetrics(METRIC_AGGS)
            .withProjections(PROJECTIONS)
            .withRollup(rollup)
            .build(),
        DEFAULT_JSON_INPUT_FORMAT
    );
  }

  public static IncrementalIndex makeWikipediaIncrementalIndex()
  {
    return makeIncrementalIndexFromResource(
        "wikipedia/wikiticker-2015-09-12-sampled.json.gz",
        IncrementalIndexSchema.builder()
                              .withRollup(false)
                              .withTimestampSpec(new TimestampSpec("time", null, null))
                              .withDimensionsSpec(
                                  DimensionsSpec.builder()
                                                .setDimensions(
                                                    Arrays.asList(
                                                        new StringDimensionSchema("channel"),
                                                        new StringDimensionSchema("cityName"),
                                                        new StringDimensionSchema("comment"),
                                                        new StringDimensionSchema("countryIsoCode"),
                                                        new StringDimensionSchema("countryName"),
                                                        new StringDimensionSchema("isAnonymous"),
                                                        new StringDimensionSchema("isMinor"),
                                                        new StringDimensionSchema("isNew"),
                                                        new StringDimensionSchema("isRobot"),
                                                        new StringDimensionSchema("isUnpatrolled"),
                                                        new StringDimensionSchema("metroCode"),
                                                        new StringDimensionSchema("namespace"),
                                                        new StringDimensionSchema("page"),
                                                        new StringDimensionSchema("regionIsoCode"),
                                                        new StringDimensionSchema("regionName"),
                                                        new StringDimensionSchema("user"),
                                                        new LongDimensionSchema("delta"),
                                                        new LongDimensionSchema("added"),
                                                        new LongDimensionSchema("deleted")
                                                    )
                                                )
                                                .build()
                              )
                              .build(),
        DEFAULT_JSON_INPUT_FORMAT
    );
  }

  public static IncrementalIndex makeIncrementalIndexFromResource(
      final String resourceFilename,
      IncrementalIndexSchema schema,
      InputFormat inputFormat
  )
  {
    File tmpDir = null;
    try {
      tmpDir = FileUtils.createTempDir("test-index-input-source");
      return IndexBuilder
          .create()
          .tmpDir(tmpDir)
          .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
          .schema(schema)
          .inputSource(ResourceInputSource.of(TestIndex.class.getClassLoader(), resourceFilename))
          .inputFormat(inputFormat)
          .inputTmpDir(new File(tmpDir, resourceFilename))
          .buildIncrementalIndex();
    }
    finally {
      try {
        FileUtils.deleteDirectory(tmpDir);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static QueryableIndex persistAndMemoryMap(IncrementalIndex index)
  {
    return persistAndMemoryMap(index, INDEX_SPEC);
  }

  public static QueryableIndex persistAndMemoryMap(IncrementalIndex index, IndexSpec indexSpec)
  {
    try {
      File someTmpFile = File.createTempFile("billy", "yay");
      someTmpFile.delete();
      FileUtils.mkdirp(someTmpFile);
      someTmpFile.deleteOnExit();

      INDEX_MERGER.persist(index, someTmpFile, indexSpec, null);
      return INDEX_IO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public static CharSource getResourceCharSource(final String resourceFilename)
  {
    final URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceFilename);
    }
    log.info("Realtime loading index file[%s]", resource);
    return Resources.asByteSource(resource).asCharSource(StandardCharsets.UTF_8);
  }

  public static IncrementalIndex makeIncrementalIndexFromTsvCharSource(final CharSource source)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
        .withTimestampSpec(new TimestampSpec("ds", "auto", null))
        .withDimensionsSpec(DIMENSIONS_SPEC)
        .withVirtualColumns(VIRTUAL_COLUMNS)
        .withMetrics(METRIC_AGGS)
        .withProjections(PROJECTIONS)
        .withRollup(true)
        .build();
    final IncrementalIndex retVal = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(10000)
        .build();

    try {
      return loadIncrementalIndexFromTsvCharSource(retVal, source);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static IncrementalIndex loadIncrementalIndexFromTsvCharSource(
      final IncrementalIndex retVal,
      final CharSource source
  ) throws IOException
  {
    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("ts", "iso", null),
            DIMENSIONS_SPEC,
            "\t",
            "\u0001",
            Arrays.asList(COLUMNS),
            false,
            0
        ),
        "utf8"
    );
    return loadIncrementalIndexFromCharSource(() -> retVal, source, parser);
  }

  public static IncrementalIndex loadIncrementalIndexFromCharSource(
      final Supplier<IncrementalIndex> indexSupplier,
      final CharSource source,
      final StringInputRowParser parser
  ) throws IOException
  {
    final IncrementalIndex retVal = indexSupplier.get();
    final AtomicLong startTime = new AtomicLong();
    int lineCount = source.readLines(
        new LineProcessor<>()
        {
          boolean runOnce = false;
          int lineCount = 0;

          @Override
          public boolean processLine(String line)
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
}
