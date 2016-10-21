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
import com.google.common.hash.Hashing;
import com.google.common.io.CharSource;
import com.google.common.io.LineProcessor;
import com.google.common.io.Resources;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class TestIndex
{
  public static final String[] COLUMNS = new String[]{
      "ts",
      "market",
      "quality",
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
      "placement",
      "placementish",
      "partial_null_column",
      "null_column",
      };
  public static final String[] METRICS = new String[]{"index", "indexMin", "indexMaxPlusTen"};
  private static final Logger log = new Logger(TestIndex.class);
  private static final Interval DATA_INTERVAL = new Interval("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  public static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory(METRICS[0], METRICS[0]),
      new DoubleMinAggregatorFactory(METRICS[1], METRICS[0]),
      new DoubleMaxAggregatorFactory(METRICS[2], null, "index + 10"),
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
  private static IncrementalIndex noRollupRealtimeIndex = null;
  private static QueryableIndex mmappedIndex = null;
  private static QueryableIndex noRollupMmappedIndex = null;
  private static QueryableIndex mergedRealtime = null;

  public static IncrementalIndex getIncrementalTestIndex()
  {
    synchronized (log) {
      if (realtimeIndex != null) {
        return realtimeIndex;
      }
    }

    return realtimeIndex = makeRealtimeIndex("druid.sample.tsv");
  }

  public static IncrementalIndex getNoRollupIncrementalTestIndex()
  {
    synchronized (log) {
      if (noRollupRealtimeIndex != null) {
        return noRollupRealtimeIndex;
      }
    }

    return noRollupRealtimeIndex = makeRealtimeIndex("druid.sample.tsv", false);
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

  public static QueryableIndex getNoRollupMMappedTestIndex()
  {
    synchronized (log) {
      if (noRollupMmappedIndex != null) {
        return noRollupMmappedIndex;
      }
    }

    IncrementalIndex incrementalIndex = getNoRollupIncrementalTestIndex();
    noRollupMmappedIndex = persistRealtimeAndLoadMMapped(incrementalIndex);

    return noRollupMmappedIndex;
  }

  public static QueryableIndex mergedRealtimeIndex()
  {
    synchronized (log) {
      if (mergedRealtime != null) {
        return mergedRealtime;
      }

      try {
        IncrementalIndex top = makeRealtimeIndex("druid.sample.tsv.top");
        IncrementalIndex bottom = makeRealtimeIndex("druid.sample.tsv.bottom");

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
                true,
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

  public static IncrementalIndex makeRealtimeIndex(final String resourceFilename)
  {
    return makeRealtimeIndex(resourceFilename, true);
  }

  public static IncrementalIndex makeRealtimeIndex(final String resourceFilename, boolean rollup)
  {
    final URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceFilename);
    }
    log.info("Realtime loading index file[%s]", resource);
    CharSource stream = Resources.asByteSource(resource).asCharSource(Charsets.UTF_8);
    return makeRealtimeIndex(stream, rollup);
  }

  public static IncrementalIndex makeRealtimeIndex(final CharSource source)
  {
    return makeRealtimeIndex(source, true);
  }

  public static IncrementalIndex makeRealtimeIndex(final CharSource source, boolean rollup)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
        .withTimestampSpec(new TimestampSpec("ds", "auto", null))
        .withQueryGranularity(QueryGranularities.NONE)
        .withMetrics(METRIC_AGGS)
        .withRollup(rollup)
        .build();
    final IncrementalIndex retVal = new OnheapIncrementalIndex(schema, true, 10000);

    try {
      return loadIncrementalIndex(retVal, source);
    }
    catch (Exception e) {
      if (rollup) {
        realtimeIndex = null;
      } else {
        noRollupRealtimeIndex = null;
      }
      throw Throwables.propagate(e);
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
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList(DIMENSIONS)), null, null),
            "\t",
            "\u0001",
            Arrays.asList(COLUMNS)
        )
        , "utf8"
    );
    return loadIncrementalIndex(retVal, source, parser);
  }

  public static IncrementalIndex loadIncrementalIndex(
      final IncrementalIndex retVal,
      final CharSource source,
      final StringInputRowParser parser
  ) throws IOException
  {
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

      INDEX_MERGER.persist(index, someTmpFile, indexSpec);
      return INDEX_IO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
