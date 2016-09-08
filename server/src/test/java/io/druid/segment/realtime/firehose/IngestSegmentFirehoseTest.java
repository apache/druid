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
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TestHelper;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
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

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private IndexIO indexIO = TestHelper.getTestIndexIO();
  private IndexMerger indexMerger = TestHelper.getTestIndexMerger();

  @Test
  public void testSanity() throws Exception
  {
    File segmentDir = tempFolder.newFolder();
    createTestIndex(segmentDir);

    QueryableIndex qi = null;
    try {
      qi = indexIO.loadIndex(segmentDir);
      StorageAdapter sa = new QueryableIndexStorageAdapter(qi);
      WindowedStorageAdapter wsa = new WindowedStorageAdapter(sa, sa.getInterval());
      IngestSegmentFirehose firehose = new IngestSegmentFirehose(
          ImmutableList.of(wsa, wsa),
          ImmutableList.of("host"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          null,
          QueryGranularities.NONE
      );

      int count = 0;
      while (firehose.hasMore()) {
        firehose.nextRow();
        count++;
      }
      Assert.assertEquals(18, count);
    }
    finally {
      if (qi != null) {
        qi.close();
      }
    }
  }

  private void createTestIndex(File segmentDir) throws Exception
  {
    List<String> rows = Lists.newArrayList(
        "2014102200,host1,10",
        "2014102200,host2,20",
        "2014102200,host3,30",
        "2014102201,host1,10",
        "2014102201,host2,20",
        "2014102201,host3,30",
        "2014102202,host1,10",
        "2014102202,host2,20",
        "2014102202,host3,30"
    );

    StringInputRowParser parser = new StringInputRowParser(
        new CSVParseSpec(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
            null,
            ImmutableList.of("timestamp", "host", "visited")
        ),
        Charsets.UTF_8.toString()
    );

    AggregatorFactory[] aggregators = new AggregatorFactory[]{
        new LongSumAggregatorFactory("visited_sum", "visited")
    };

    IncrementalIndex index = null;
    try {
      index = new OnheapIncrementalIndex(0, QueryGranularities.NONE, aggregators, true, true, true, 5000);
      for (String line : rows) {
        index.add(parser.parse(line));
      }
      indexMerger.persist(index, segmentDir, new IndexSpec());
    }
    finally {
      if (index != null) {
        index.close();
      }
    }
  }

}
