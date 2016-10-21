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
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
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
public class DeterminePartitionsJobTest
{

  private HadoopDruidIndexerConfig config;
  private int expectedNumOfSegments;
  private int[] expectedNumOfShardsForEachSegment;
  private String[][][] expectedStartEndForEachShard;
  private File dataFile;
  private File tmpDir;

  @Parameterized.Parameters(name = "assumeGrouped={0}, "
                                   + "targetPartitionSize={1}, "
                                   + "interval={2}"
                                   + "expectedNumOfSegments={3}, "
                                   + "expectedNumOfShardsForEachSegment={4}, "
                                   + "expectedStartEndForEachShard={5}, "
                                   + "data={6}")
  public static Collection<Object[]> constructFeed()
  {
    return Arrays.asList(
        new Object[][]{
            {
                true,
                3L,
                "2014-10-22T00:00:00Z/P1D",
                1,
                new int[]{5},
                new String[][][]{
                    {
                        { null, "c.example.com" },
                        { "c.example.com", "e.example.com" },
                        { "e.example.com", "g.example.com" },
                        { "g.example.com", "i.example.com" },
                        { "i.example.com", null }
                    }
                },
                ImmutableList.of(
                    "2014102200,a.example.com,CN,100",
                    "2014102200,b.exmaple.com,US,50",
                    "2014102200,c.example.com,US,200",
                    "2014102200,d.example.com,US,250",
                    "2014102200,e.example.com,US,123",
                    "2014102200,f.example.com,US,567",
                    "2014102200,g.example.com,US,11",
                    "2014102200,h.example.com,US,251",
                    "2014102200,i.example.com,US,963",
                    "2014102200,j.example.com,US,333"
                )
            },
            {
                false,
                3L,
                "2014-10-20T00:00:00Z/P1D",
                1,
                new int[]{5},
                new String[][][]{
                    {
                        { null, "c.example.com"},
                        { "c.example.com", "e.example.com" },
                        { "e.example.com", "g.example.com" },
                        { "g.example.com", "i.example.com" },
                        { "i.example.com", null }
                    }
                },
                ImmutableList.of(
                    "2014102000,a.example.com,CN,100",
                    "2014102000,a.example.com,CN,100",
                    "2014102000,b.exmaple.com,US,50",
                    "2014102000,b.exmaple.com,US,50",
                    "2014102000,c.example.com,US,200",
                    "2014102000,c.example.com,US,200",
                    "2014102000,d.example.com,US,250",
                    "2014102000,d.example.com,US,250",
                    "2014102000,e.example.com,US,123",
                    "2014102000,e.example.com,US,123",
                    "2014102000,f.example.com,US,567",
                    "2014102000,f.example.com,US,567",
                    "2014102000,g.example.com,US,11",
                    "2014102000,g.example.com,US,11",
                    "2014102000,h.example.com,US,251",
                    "2014102000,h.example.com,US,251",
                    "2014102000,i.example.com,US,963",
                    "2014102000,i.example.com,US,963",
                    "2014102000,j.example.com,US,333",
                    "2014102000,j.example.com,US,333"
                )
            },
            {
                true,
                6L,
                "2014-10-20T00:00:00Z/P3D",
                3,
                new int[]{2, 2, 2},
                new String[][][]{
                    {
                        { null, "f.example.com" },
                        { "f.example.com", null }
                    },
                    {
                        { null, "f.example.com" },
                        { "f.example.com", null }
                    },
                    {
                        { null, "f.example.com" },
                        { "f.example.com", null }
                    }
                },
                ImmutableList.of(
                    "2014102000,a.example.com,CN,100",
                    "2014102000,b.exmaple.com,CN,50",
                    "2014102000,c.example.com,CN,200",
                    "2014102000,d.example.com,US,250",
                    "2014102000,e.example.com,US,123",
                    "2014102000,f.example.com,US,567",
                    "2014102000,g.example.com,US,11",
                    "2014102000,h.example.com,US,251",
                    "2014102000,i.example.com,US,963",
                    "2014102000,j.example.com,US,333",
                    "2014102000,k.example.com,US,555",
                    "2014102100,a.example.com,CN,100",
                    "2014102100,b.exmaple.com,CN,50",
                    "2014102100,c.example.com,CN,200",
                    "2014102100,d.example.com,US,250",
                    "2014102100,e.example.com,US,123",
                    "2014102100,f.example.com,US,567",
                    "2014102100,g.example.com,US,11",
                    "2014102100,h.example.com,US,251",
                    "2014102100,i.example.com,US,963",
                    "2014102100,j.example.com,US,333",
                    "2014102100,k.example.com,US,555",
                    "2014102200,a.example.com,CN,100",
                    "2014102200,b.exmaple.com,CN,50",
                    "2014102200,c.example.com,CN,200",
                    "2014102200,d.example.com,US,250",
                    "2014102200,e.example.com,US,123",
                    "2014102200,f.example.com,US,567",
                    "2014102200,g.example.com,US,11",
                    "2014102200,h.example.com,US,251",
                    "2014102200,i.example.com,US,963",
                    "2014102200,j.example.com,US,333",
                    "2014102200,k.example.com,US,555"
                )
            }
        }
    );
  }

  public DeterminePartitionsJobTest(
      boolean assumeGrouped,
      Long targetPartitionSize,
      String interval,
      int expectedNumOfSegments,
      int[] expectedNumOfShardsForEachSegment,
      String[][][] expectedStartEndForEachShard,
      List<String> data
  ) throws IOException
  {
    this.expectedNumOfSegments = expectedNumOfSegments;
    this.expectedNumOfShardsForEachSegment = expectedNumOfShardsForEachSegment;
    this.expectedStartEndForEachShard = expectedStartEndForEachShard;

    dataFile = File.createTempFile("test_website_data", "tmp");
    dataFile.deleteOnExit();
    tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();

    FileUtils.writeLines(dataFile, data);

    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                "website",
                HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host", "country")), null, null),
                            null,
                            ImmutableList.of("timestamp", "host", "country", "visited_num")
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{new LongSumAggregatorFactory("visited_num", "visited_num")},
                new UniformGranularitySpec(
                    Granularity.DAY, QueryGranularities.NONE, ImmutableList.of(new Interval(interval))
                ),
                HadoopDruidIndexerConfig.JSON_MAPPER
            ),
            new HadoopIOConfig(
                ImmutableMap.<String, Object>of(
                    "paths",
                    dataFile.getCanonicalPath(),
                    "type",
                    "static"
                ),
                null,
                tmpDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                new SingleDimensionPartitionsSpec(null, targetPartitionSize, null, assumeGrouped),
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
        )
    );
  }

  @Test
  public void testPartitionJob()
  {
    DeterminePartitionsJob job = new DeterminePartitionsJob(config);
    job.run();

    int shardNum = 0;
    int segmentNum = 0;
    Assert.assertEquals(expectedNumOfSegments, config.getSchema().getTuningConfig().getShardSpecs().size());

    for (Map.Entry<DateTime, List<HadoopyShardSpec>> entry : config.getSchema()
                                                                   .getTuningConfig()
                                                                   .getShardSpecs()
                                                                   .entrySet()) {
      int partitionNum = 0;
      List<HadoopyShardSpec> specs = entry.getValue();
      Assert.assertEquals(expectedNumOfShardsForEachSegment[segmentNum], specs.size());

      for (HadoopyShardSpec spec : specs) {
        SingleDimensionShardSpec actualSpec = (SingleDimensionShardSpec) spec.getActualSpec();
        Assert.assertEquals(shardNum, spec.getShardNum());
        Assert.assertEquals(expectedStartEndForEachShard[segmentNum][partitionNum][0], actualSpec.getStart());
        Assert.assertEquals(expectedStartEndForEachShard[segmentNum][partitionNum][1], actualSpec.getEnd());
        Assert.assertEquals(partitionNum, actualSpec.getPartitionNum());
        shardNum++;
        partitionNum++;
      }

      segmentNum++;
    }
  }

  @After
  public void tearDown() throws Exception
  {
    FileUtils.forceDelete(dataFile);
    FileUtils.deleteDirectory(tmpDir);
  }
}
