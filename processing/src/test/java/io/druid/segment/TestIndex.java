/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;
import com.google.common.io.InputSupplier;
import com.google.common.io.LineProcessor;
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
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class TestIndex
{
  public static final String[] COLUMNS = new String[]{
      "ts",
      "provider",
      "quALIty",
      "plAcEmEnT",
      "pLacementish",
      "iNdEx",
      "qualiTy_Uniques"
  };
  public static final String[] DIMENSIONS = new String[]{"provider", "quALIty", "plAcEmEnT", "pLacementish"};
  public static final String[] METRICS = new String[]{"iNdEx"};
  private static final Logger log = new Logger(TestIndex.class);
  private static final Interval DATA_INTERVAL = new Interval("2011-01-12T00:00:00.000Z/2011-05-01T00:00:00.000Z");
  private static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory(METRICS[0], METRICS[0]),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };

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

        IndexMaker.persist(top, DATA_INTERVAL, topFile);
        IndexMaker.persist(bottom, DATA_INTERVAL, bottomFile);

        mergedRealtime = IndexIO.loadIndex(
            IndexMaker.mergeQueryableIndex(
                Arrays.asList(IndexIO.loadIndex(topFile), IndexIO.loadIndex(bottomFile)),
                METRIC_AGGS,
                mergedFile
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
    log.info("Realtime loading index file[%s]", resource);
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
                                          .withMinTimestamp(new DateTime("2011-01-12T00:00:00.000Z").getMillis())
                                          .withQueryGranularity(QueryGranularity.NONE)
                                          .withMetrics(METRIC_AGGS)
                                          .build();
    final IncrementalIndex retVal;
    if (useOffheap) {
      retVal = new OffheapIncrementalIndex(
          schema,
          TestQueryRunners.pool
      );
    } else {
      retVal = new IncrementalIndex(
          schema,
          TestQueryRunners.pool
      );
    }

    final AtomicLong startTime = new AtomicLong();
    int lineCount;
    try {
      lineCount = CharStreams.readLines(
          CharStreams.newReaderSupplier(
              new InputSupplier<InputStream>()
              {
                @Override
                public InputStream getInput() throws IOException
                {
                  return resource.openStream();
                }
              },
              Charsets.UTF_8
          ),
          new LineProcessor<Integer>()
          {
            StringInputRowParser parser = new StringInputRowParser(
                new DelimitedParseSpec(
                    new TimestampSpec("ts", "iso"),
                    new DimensionsSpec(Arrays.asList(DIMENSIONS), null, null),
                    "\t",
                    "\u0001",
                    Arrays.asList(COLUMNS)
                ),
                null, null, null, null
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

      IndexMaker.persist(index, someTmpFile);
      return IndexIO.loadIndex(someTmpFile);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
