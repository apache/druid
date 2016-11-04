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

package io.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class DetermineHashedPartitionsJobTest
{
  private HadoopDruidIndexerConfig indexerConfig;
  private int expectedNumTimeBuckets;
  private int[] expectedNumOfShards;
  private int errorMargin;

  @Parameterized.Parameters(name = "File={0}, TargetPartitionSize={1}, Interval={2}, ErrorMargin={3}, NumTimeBuckets={4}, NumShards={5}")
  public static Collection<?> data(){
    int[] first = new int[1];
    Arrays.fill(first, 13);
    int[] second = new int[6];
    Arrays.fill(second, 1);
    int[] third = new int[6];
    Arrays.fill(third, 13);
    third[2] = 12;
    third[5] = 11;

    return Arrays.asList(
        new Object[][]{
            {
                DetermineHashedPartitionsJobTest.class.getClass().getResource("/druid.test.data.with.duplicate.rows.tsv").getPath(),
                1L,
                "2011-04-10T00:00:00.000Z/2011-04-11T00:00:00.000Z",
                0,
                1,
                first
            },
            {
                DetermineHashedPartitionsJobTest.class.getClass().getResource("/druid.test.data.with.duplicate.rows.tsv").getPath(),
                100L,
                "2011-04-10T00:00:00.000Z/2011-04-16T00:00:00.000Z",
                0,
                6,
                second
            },
            {
                DetermineHashedPartitionsJobTest.class.getClass().getResource("/druid.test.data.with.duplicate.rows.tsv").getPath(),
                1L,
                "2011-04-10T00:00:00.000Z/2011-04-16T00:00:00.000Z",
                0,
                6,
                third
            }
        }
    );
  }

  public DetermineHashedPartitionsJobTest(String dataFilePath, long targetPartitionSize, String interval, int errorMargin, int expectedNumTimeBuckets, int[] expectedNumOfShards) throws IOException
  {
    this.expectedNumOfShards = expectedNumOfShards;
    this.expectedNumTimeBuckets = expectedNumTimeBuckets;
    this.errorMargin = errorMargin;
    File tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    HadoopIngestionSpec ingestionSpec = new HadoopIngestionSpec(
        new DataSchema(
            "test_schema",
            HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                new StringInputRowParser(
                    new DelimitedParseSpec(
                        new TimestampSpec("ts", null, null),
                        new DimensionsSpec(
                            DimensionsSpec.getDefaultSchemas(ImmutableList.of("market", "quality", "placement", "placementish")),
                            null,
                            null
                        ),
                        "\t",
                        null,
                        Arrays.asList(
                            "ts",
                            "market",
                            "quality",
                            "placement",
                            "placementish",
                            "index"
                        )
                    ),
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{new DoubleSumAggregatorFactory("index", "index")},
            new UniformGranularitySpec(
                Granularity.DAY,
                QueryGranularities.NONE,
                ImmutableList.of(new Interval(interval))
            ),
            HadoopDruidIndexerConfig.JSON_MAPPER
        ),
        new HadoopIOConfig(
            ImmutableMap.<String, Object>of(
                "paths",
                dataFilePath,
                "type",
                "static"
            ), null, tmpDir.getAbsolutePath()
        ),
        new HadoopTuningConfig(
            tmpDir.getAbsolutePath(),
            null,
            new HashedPartitionsSpec(targetPartitionSize, null, true, null, null),
            null,
            null,
            null,
            false,
            false,
            false,
            false,
            null,
            false,
            false,
            null,
            null,
            null,
            false,
            false
        )
    );
    this.indexerConfig = new HadoopDruidIndexerConfig(ingestionSpec);
  }

  @Test
  public void testDetermineHashedPartitions(){
    DetermineHashedPartitionsJob determineHashedPartitionsJob = new DetermineHashedPartitionsJob(indexerConfig);
    determineHashedPartitionsJob.run();
    Map<DateTime, List<HadoopyShardSpec>> shardSpecs = indexerConfig.getSchema().getTuningConfig().getShardSpecs();
    Assert.assertEquals(
        expectedNumTimeBuckets,
        shardSpecs.entrySet().size()
    );
    int i=0;
    for(Map.Entry<DateTime, List<HadoopyShardSpec>> entry : shardSpecs.entrySet()) {
      Assert.assertEquals(
          expectedNumOfShards[i++],
          entry.getValue().size(),
          errorMargin
      );
    }
  }
}
