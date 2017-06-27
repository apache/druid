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

package io.druid.segment.realtime.firehose;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.collections.spatial.search.RadiusBound;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.NewSpatialDimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.hll.HyperLogLogCollector;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.filter.SpatialDimFilter;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

/**
 */
public class IngestSegmentFirehoseTest
{
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      ImmutableList.of(
          new StringDimensionSchema("host"),
          new NewSpatialDimensionSchema("spatial", ImmutableList.of("x", "y"))
      ),
      null,
      null
  );

  private static final DimensionsSpec DIMENSIONS_SPEC_REINDEX = new DimensionsSpec(
      ImmutableList.of(
          new StringDimensionSchema("host"),
          new NewSpatialDimensionSchema("spatial", ImmutableList.of("spatial"))
      ),
      null,
      null
  );

  private static final List<AggregatorFactory> AGGREGATORS = ImmutableList.of(
      new LongSumAggregatorFactory("visited_sum", "visited"),
      new HyperUniquesAggregatorFactory("unique_hosts", "host")
  );

  private static final List<AggregatorFactory> AGGREGATORS_REINDEX = ImmutableList.of(
      new LongSumAggregatorFactory("visited_sum", "visited_sum"),
      new HyperUniquesAggregatorFactory("unique_hosts", "unique_hosts")
  );

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private IndexIO indexIO = TestHelper.getTestIndexIO();
  private IndexMerger indexMerger = TestHelper.getTestIndexMergerV9();

  @Test
  public void testReadFromIndexAndWriteAnotherIndex() throws Exception
  {
    // Tests a "reindexing" use case that is a common use of ingestSegment.

    File segmentDir = tempFolder.newFolder();
    createTestIndex(segmentDir);

    try (
        final QueryableIndex qi = indexIO.loadIndex(segmentDir);
        final IncrementalIndex index = new IncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(DIMENSIONS_SPEC_REINDEX)
                    .withMetrics(AGGREGATORS_REINDEX.toArray(new AggregatorFactory[]{}))
                    .build()
            )
        .setMaxRowCount(5000)
        .buildOnheap();
    ) {
      final StorageAdapter sa = new QueryableIndexStorageAdapter(qi);
      final WindowedStorageAdapter wsa = new WindowedStorageAdapter(sa, sa.getInterval());
      final IngestSegmentFirehose firehose = new IngestSegmentFirehose(
          ImmutableList.of(wsa, wsa),
          ImmutableList.of("host", "spatial"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          null
      );

      int count = 0;
      while (firehose.hasMore()) {
        final InputRow row = firehose.nextRow();
        Assert.assertNotNull(row);
        if (count == 0) {
          Assert.assertEquals(new DateTime("2014-10-22T00Z"), row.getTimestamp());
          Assert.assertEquals("host1", row.getRaw("host"));
          Assert.assertEquals("0,1", row.getRaw("spatial"));
          Assert.assertEquals(10L, row.getRaw("visited_sum"));
          Assert.assertEquals(1.0d, ((HyperLogLogCollector) row.getRaw("unique_hosts")).estimateCardinality(), 0.1);
        }
        count++;
        index.add(row);
      }
      Assert.assertEquals(18, count);

      // Check the index
      Assert.assertEquals(9, index.size());
      final IncrementalIndexStorageAdapter queryable = new IncrementalIndexStorageAdapter(index);
      Assert.assertEquals(2, queryable.getAvailableDimensions().size());
      Assert.assertEquals("host", queryable.getAvailableDimensions().get(0));
      Assert.assertEquals("spatial", queryable.getAvailableDimensions().get(1));
      Assert.assertEquals(ImmutableList.of("visited_sum", "unique_hosts"), queryable.getAvailableMetrics());

      // Do a spatial filter
      final IngestSegmentFirehose firehose2 = new IngestSegmentFirehose(
          ImmutableList.of(new WindowedStorageAdapter(queryable, new Interval("2000/3000"))),
          ImmutableList.of("host", "spatial"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          new SpatialDimFilter("spatial", new RadiusBound(new float[]{1, 0}, 0.1f))
      );
      final InputRow row = firehose2.nextRow();
      Assert.assertFalse(firehose2.hasMore());
      Assert.assertEquals(new DateTime("2014-10-22T00Z"), row.getTimestamp());
      Assert.assertEquals("host2", row.getRaw("host"));
      Assert.assertEquals("1,0", row.getRaw("spatial"));
      Assert.assertEquals(40L, row.getRaw("visited_sum"));
      Assert.assertEquals(1.0d, ((HyperLogLogCollector) row.getRaw("unique_hosts")).estimateCardinality(), 0.1);
    }
  }

  private void createTestIndex(File segmentDir) throws Exception
  {
    final List<String> rows = Lists.newArrayList(
        "2014102200\thost1\t10\t0\t1",
        "2014102200\thost2\t20\t1\t0",
        "2014102200\thost3\t30\t1\t1",
        "2014102201\thost1\t10\t1\t1",
        "2014102201\thost2\t20\t1\t1",
        "2014102201\thost3\t30\t1\t1",
        "2014102202\thost1\t10\t1\t1",
        "2014102202\thost2\t20\t1\t1",
        "2014102202\thost3\t30\t1\t1"
    );

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            DIMENSIONS_SPEC,
            "\t",
            null,
            ImmutableList.of("timestamp", "host", "visited", "x", "y", "spatial"),
            false,
            0
        ),
        Charsets.UTF_8.toString()
    );

    try (
        final IncrementalIndex index = new IncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(parser.getParseSpec().getDimensionsSpec())
                    .withMetrics(AGGREGATORS.toArray(new AggregatorFactory[]{}))
                    .build()
            )
        .setMaxRowCount(5000)
        .buildOnheap();
    ) {
      for (String line : rows) {
        index.add(parser.parse(line));
      }
      indexMerger.persist(index, segmentDir, new IndexSpec());
    }
  }
}
