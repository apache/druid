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
import com.metamx.common.logger.Logger;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OffheapIncrementalIndex;
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
      "quality_uniques"
  };
  public static final String[] DIMENSIONS = new String[]{
      "market",
      "quality",
      "placement",
      "placementish",
      "partial_null_column",
      "null_column",
  };
  public static final String[] METRICS = new String[]{"index"};
  private static final Logger log = new Logger(TestIndex.class);
  private static final Interval DATA_INTERVAL = new Interval("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  private static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory(METRICS[0], METRICS[0]),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };
  private static final IndexSpec indexSpec = new IndexSpec();

  static {
    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }
  }

  private static IncrementalIndex realtimeIndex = null;
  private static QueryableIndex mmappedIndex = null;
  private static QueryableIndex mergedRealtime = null;

  public static IncrementalIndex getIncrementalTestIndex(boolean useOffheap)
  {
    synchronized (log) {
      if (realtimeIndex != null) {
        return realtimeIndex;
      }
    }

    return realtimeIndex = makeRealtimeIndex("druid.sample.tsv", useOffheap);
  }

  public static QueryableIndex getMMappedTestIndex()
  {
    synchronized (log) {
      if (mmappedIndex != null) {
        return mmappedIndex;
      }
    }

    IncrementalIndex incrementalIndex = getIncrementalTestIndex(false);
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
        IncrementalIndex top = makeRealtimeIndex("druid.sample.tsv.top", false);
        IncrementalIndex bottom = makeRealtimeIndex("druid.sample.tsv.bottom", false);

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

        IndexMerger.persist(top, DATA_INTERVAL, topFile, null, indexSpec);
        IndexMerger.persist(bottom, DATA_INTERVAL, bottomFile, null, indexSpec);

        mergedRealtime = IndexIO.loadIndex(
            IndexMerger.mergeQueryableIndex(
                Arrays.asList(IndexIO.loadIndex(topFile), IndexIO.loadIndex(bottomFile)),
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

  private static IncrementalIndex makeRealtimeIndex(final String resourceFilename, final boolean useOffheap)
  {
    final URL resource = TestIndex.class.getClassLoader().getResource(resourceFilename);
    if (resource == null) {
      throw new IllegalArgumentException("cannot find resource " + resourceFilename);
    }
    log.info("Realtime loading index file[%s]", resource);
    CharSource stream = Resources.asByteSource(resource).asCharSource(Charsets.UTF_8);
    return makeRealtimeIndex(stream, useOffheap);
  }

  public static IncrementalIndex makeRealtimeIndex(final CharSource source, final boolean useOffheap)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
        .withQueryGranularity(QueryGranularity.NONE)
        .withMetrics(METRIC_AGGS)
        .build();
    final IncrementalIndex retVal;
    if (useOffheap) {
      retVal = new OffheapIncrementalIndex(
          schema,
          TestQueryRunners.pool,
          true,
          100 * 1024 * 1024
      );
    } else {
      retVal = new OnheapIncrementalIndex(
          schema,
          10000
      );
    }

    final AtomicLong startTime = new AtomicLong();
    int lineCount;
    try {
      lineCount = source.readLines(
          new LineProcessor<Integer>()
          {
            StringInputRowParser parser = new StringInputRowParser(
                new DelimitedParseSpec(
                    new TimestampSpec("ts", "iso", null),
                    new DimensionsSpec(Arrays.asList(DIMENSIONS), null, null),
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

      IndexMerger.persist(index, someTmpFile, null, indexSpec);
      return IndexIO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
