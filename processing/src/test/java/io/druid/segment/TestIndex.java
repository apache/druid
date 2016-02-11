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

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import com.metamx.common.logger.Logger;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class TestIndex
{
  public static final String[] COLUMNS = new String[]{
      "ts",
      "market",
      "market_long",
      "market_float",
      "quality",
      "quality_long",
      "quality_float",
      "placement",
      "placementish",
      "index",
      "partial_null_column",
      "null_column",
      "quality_uniques"
  };

  public static final String[] DIMENSIONS = new String[]{
      "market",
      "market_long",
      "market_float",
      "quality",
      "quality_long",
      "quality_float",
      "placement",
      "placementish",
      "partial_null_column",
      "null_column"
  };

  public static final DimensionsSpec.ValueType[] DIMENSION_TYPES = new DimensionsSpec.ValueType[]{
      DimensionsSpec.ValueType.STRING,
      DimensionsSpec.ValueType.LONG,
      DimensionsSpec.ValueType.FLOAT,
      DimensionsSpec.ValueType.STRING,
      DimensionsSpec.ValueType.LONG,
      DimensionsSpec.ValueType.FLOAT,
      DimensionsSpec.ValueType.STRING,
      DimensionsSpec.ValueType.STRING,
      DimensionsSpec.ValueType.STRING,
      DimensionsSpec.ValueType.STRING
  };

  private static final Map<String, String> STRING_DIM_NAME_AND_VALUES_MAP = ImmutableMap.<String, String>builder()
      .put("quality", "quality")
      .put("market", "market")
      .put("automotive", "automotive")
      .put("business", "business")
      .put("entertainment", "entertainment")
      .put("health", "health")
      .put("mezzanine", "mezzanine")
      .put("news", "news")
      .put("premium", "premium")
      .put("technology", "technology")
      .put("travel", "travel")
      .put("spot", "spot")
      .put("total_market", "total_market")
      .put("upfront", "upfront")
      .put("billyblank", "billyblank")
      .build();

  private static final Map<String, Object> LONG_DIM_NAME_AND_VALUES_MAP = ImmutableMap.<String, Object>builder()
      .put("quality", "quality_long")
      .put("market", "market_long")
      .put("automotive", 1111L)
      .put("business", 2222L)
      .put("entertainment", 3333L)
      .put("health", 4444L)
      .put("mezzanine", 5555L)
      .put("news", 6666L)
      .put("premium", 7777L)
      .put("technology", 8888L)
      .put("travel", 8899L)
      .put("spot", 1111L)
      .put("total_market", 2222L)
      .put("upfront", 3333L)
      .put("billyblank", 12345678L)
      .build();

  private static final Map<String, Object> FLOAT_DIM_NAME_AND_VALUES_MAP = ImmutableMap.<String, Object>builder()
      .put("quality", "quality_float")
      .put("market", "market_float")
      .put("automotive", 111.111f)
      .put("business", 222.222f)
      .put("entertainment", 333.333f)
      .put("health", 444.444f)
      .put("mezzanine", 555.555f)
      .put("news", 666.666f)
      .put("premium", 777.777f)
      .put("technology", 888.888f)
      .put("travel", 889.999f)
      .put("spot", 111.111f)
      .put("total_market", 222.222f)
      .put("upfront", 333.333f)
      .put("billyblank", 1234.5678f)
      .build();

  public static Object getDimMappingForType(ValueType type, String lookupStr)
  {
    switch (type) {
      case STRING:
        return STRING_DIM_NAME_AND_VALUES_MAP.get(lookupStr);
      case LONG:
        return LONG_DIM_NAME_AND_VALUES_MAP.get(lookupStr);
      case FLOAT:
        return FLOAT_DIM_NAME_AND_VALUES_MAP.get(lookupStr);
      default:
        throw new UnsupportedOperationException("Invalid type: " + type);
    }
  }
  
 
  public static final String[] METRICS = new String[]{"index"};
  private static final Logger log = new Logger(TestIndex.class);
  private static final Interval DATA_INTERVAL = new Interval("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  public static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory(METRICS[0], METRICS[0]),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };
  private static final IndexSpec indexSpec = new IndexSpec();

  private static final IndexMerger INDEX_MERGER = TestHelper.getTestIndexMerger();
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();

  static {
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }
  }

  private static IncrementalIndex realtimeIndex = null;
  private static QueryableIndex mmappedIndex = null;
  private static QueryableIndex mergedRealtime = null;

  public static IncrementalIndex getIncrementalTestIndex()
  {
    synchronized (log) {
      if (realtimeIndex != null) {
        return realtimeIndex;
      }
    }

    return realtimeIndex = makeRealtimeIndex("druid.sample.numericdims.tsv");
  }

  public static QueryableIndex getMMappedTestIndex()
  {
    synchronized (log) {
      if (mmappedIndex != null) {
        return mmappedIndex;
      }
    }

    IncrementalIndex incrementalIndex = getIncrementalTestIndex();
    mmappedIndex = persistRealtimeAndLoadMMapped(incrementalIndex);

    return mmappedIndex;
  }

  public static QueryableIndex mergedRealtimeIndex()
  {
    synchronized (log) {
      if (mergedRealtime != null) {
        return mergedRealtime;
      }

      try {
        IncrementalIndex top = makeRealtimeIndex("druid.sample.numericdims.tsv.top");
        IncrementalIndex bottom = makeRealtimeIndex("druid.sample.numericdims.tsv.bottom");

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

        INDEX_MERGER.persist(top, DATA_INTERVAL, topFile, indexSpec);
        INDEX_MERGER.persist(bottom, DATA_INTERVAL, bottomFile, indexSpec);

        mergedRealtime = INDEX_IO.loadIndex(
            INDEX_MERGER.mergeQueryableIndex(
                Arrays.asList(INDEX_IO.loadIndex(topFile), INDEX_IO.loadIndex(bottomFile)),
                METRIC_AGGS,
                mergedFile,
                indexSpec
            )
        );

        return mergedRealtime;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static IncrementalIndex makeRealtimeIndex(final String resourceFilename)
  {
    final URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceFilename);
    }
    log.info("Realtime loading index file[%s]", resource);
    CharSource stream = Resources.asByteSource(resource).asCharSource(Charsets.UTF_8);
    return makeRealtimeIndex(stream);
  }

  public static IncrementalIndex makeRealtimeIndex(final CharSource source)
  {
    final DimensionsSpec dimSpec = new DimensionsSpec(Arrays.asList(DIMENSIONS), null, null, Arrays.asList(DIMENSION_TYPES));
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(dimSpec)
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
        .withQueryGranularity(QueryGranularity.NONE)
        .withMetrics(METRIC_AGGS)
        .build();
    final IncrementalIndex retVal = new OnheapIncrementalIndex(
        schema,
        10000
    );

    final AtomicLong startTime = new AtomicLong();
    int lineCount;
    try {
      lineCount = source.readLines(
          new LineProcessor<Integer>()
          {
            StringInputRowParser parser = new StringInputRowParser(
                new DelimitedParseSpec(
                    new TimestampSpec("ts", "iso", null),
                    dimSpec,
                    "\t",
                    "\u0001",
                    Arrays.asList(COLUMNS)
                )
            );
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
    }
    catch (IOException e) {
      realtimeIndex = null;
      throw Throwables.propagate(e);
    }

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

      INDEX_MERGER.persist(index, someTmpFile, indexSpec);
      return INDEX_IO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
