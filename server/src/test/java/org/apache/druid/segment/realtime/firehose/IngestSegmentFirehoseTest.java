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

package org.apache.druid.segment.realtime.firehose;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.collections.spatial.search.RadiusBound;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DelimitedParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.NewSpatialDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.filter.SpatialDimFilter;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
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

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final IndexIO indexIO;
  private final IndexMerger indexMerger;

  public IngestSegmentFirehoseTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexIO = TestHelper.getTestIndexIO();
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
  }

  @Test
  public void testReadFromIndexAndWriteAnotherIndex() throws Exception
  {
    // Tests a "reindexing" use case that is a common use of ingestSegment.

    File segmentDir = tempFolder.newFolder();
    createTestIndex(segmentDir);

    try (
        final QueryableIndex qi = indexIO.loadIndex(segmentDir);
        final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(DIMENSIONS_SPEC_REINDEX)
                    .withMetrics(AGGREGATORS_REINDEX.toArray(new AggregatorFactory[0]))
                    .build()
            )
            .setMaxRowCount(5000)
            .build()
    ) {
      final StorageAdapter sa = new QueryableIndexStorageAdapter(qi);
      final WindowedStorageAdapter wsa = new WindowedStorageAdapter(sa, sa.getInterval());
      final IngestSegmentFirehose firehose = new IngestSegmentFirehose(
          ImmutableList.of(wsa, wsa),
          TransformSpec.NONE,
          ImmutableList.of("host", "spatial"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          null
      );

      int count = 0;
      while (firehose.hasMore()) {
        final InputRow row = firehose.nextRow();
        Assert.assertNotNull(row);
        if (count == 0) {
          Assert.assertEquals(DateTimes.of("2014-10-22T00Z"), row.getTimestamp());
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
          ImmutableList.of(new WindowedStorageAdapter(queryable, Intervals.of("2000/3000"))),
          TransformSpec.NONE,
          ImmutableList.of("host", "spatial"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          new SpatialDimFilter("spatial", new RadiusBound(new float[]{1, 0}, 0.1f))
      );
      final InputRow row = firehose2.nextRow();
      Assert.assertFalse(firehose2.hasMore());
      Assert.assertEquals(DateTimes.of("2014-10-22T00Z"), row.getTimestamp());
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
        StandardCharsets.UTF_8.toString()
    );

    try (
        final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(parser.getParseSpec().getDimensionsSpec())
                    .withMetrics(AGGREGATORS.toArray(new AggregatorFactory[0]))
                    .build()
            )
            .setMaxRowCount(5000)
            .build()
    ) {
      for (String line : rows) {
        index.add(parser.parse(line));
      }
      indexMerger.persist(index, segmentDir, new IndexSpec(), null);
    }
  }
}
