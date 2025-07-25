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

package org.apache.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class DeterminePartitionsJobTest
{
  @Nullable
  private static final Long NO_TARGET_ROWS_PER_SEGMENT = null;
  @Nullable
  private static final Long NO_MAX_ROWS_PER_SEGMENT = null;

  private final HadoopDruidIndexerConfig config;
  private final int expectedNumOfSegments;
  private final int[] expectedNumOfShardsForEachSegment;
  private final String[][][] expectedStartEndForEachShard;
  private final File dataFile;
  private final File tmpDir;

  @Parameterized.Parameters(name = "assumeGrouped={0}, "
                                   + "targetRowsPerSegment={1}, "
                                   + "maxRowsPerSegment={2}, "
                                   + "intervals={3}"
                                   + "expectedNumOfSegments={4}, "
                                   + "expectedNumOfShardsForEachSegment={5}, "
                                   + "expectedStartEndForEachShard={6}, "
                                   + "data={7}")
  public static Collection<Object[]> constructFeed()
  {
    return Arrays.asList(
        new Object[][]{
            {
                false,
                1,
                NO_MAX_ROWS_PER_SEGMENT,
                List.of("1970-01-01T00:00:00Z/P1D"),
                1,
                new int[]{1},
                new String[][][]{
                    {
                        {null, null}
                    }
                },
                ImmutableList.of("1970010100,c.example.com,CN,100")
            },
            {
                // Test partitoning by targetRowsPerSegment
                true,
                2,
                NO_MAX_ROWS_PER_SEGMENT,
                List.of("2014-10-22T00:00:00Z/P1D"),
                1,
                new int[]{5},
                new String[][][]{
                    {
                        {null, "c.example.com"},
                        {"c.example.com", "e.example.com"},
                        {"e.example.com", "g.example.com"},
                        {"g.example.com", "i.example.com"},
                        {"i.example.com", null}
                    }
                },
                ImmutableList.of(
                    "2014102200,a.example.com,CN,100",
                    "2014102200,b.example.com,US,50",
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
                true,
                NO_TARGET_ROWS_PER_SEGMENT,
                2,
                List.of("2014-10-22T00:00:00Z/P1D"),
                1,
                new int[]{5},
                new String[][][]{
                    {
                        {null, "c.example.com"},
                        {"c.example.com", "e.example.com"},
                        {"e.example.com", "g.example.com"},
                        {"g.example.com", "i.example.com"},
                        {"i.example.com", null}
                    }
                },
                ImmutableList.of(
                    "2014102200,a.example.com,CN,100",
                    "2014102200,b.example.com,US,50",
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
                NO_TARGET_ROWS_PER_SEGMENT,
                2,
                List.of("2014-10-20T00:00:00Z/P1D"),
                1,
                new int[]{5},
                new String[][][]{
                    {
                        {null, "c.example.com"},
                        {"c.example.com", "e.example.com"},
                        {"e.example.com", "g.example.com"},
                        {"g.example.com", "i.example.com"},
                        {"i.example.com", null}
                    }
                },
                ImmutableList.of(
                    "2014102000,a.example.com,CN,100",
                    "2014102000,a.example.com,CN,100",
                    "2014102000,b.example.com,US,50",
                    "2014102000,b.example.com,US,50",
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
                NO_TARGET_ROWS_PER_SEGMENT,
                5,
                List.of("2014-10-20T00:00:00Z/P3D"),
                3,
                new int[]{2, 2, 2},
                new String[][][]{
                    {
                        {null, "f.example.com"},
                        {"f.example.com", null}
                    },
                    {
                        {null, "f.example.com"},
                        {"f.example.com", null}
                    },
                    {
                        {null, "f.example.com"},
                        {"f.example.com", null}
                    }
                },
                ImmutableList.of(
                    "2014102000,a.example.com,CN,100",
                    "2014102000,b.example.com,CN,50",
                    "2014102000,c.example.com,CN,200",
                    "2014102000,d.example.com,US,250",
                    "2014102000,e.example.com,US,123",
                    "2014102000,f.example.com,US,567",
                    "2014102000,g.example.com,US,11",
                    "2014102000,h.example.com,US,251",
                    "2014102000,i.example.com,US,963",
                    "2014102000,j.example.com,US,333",
                    "2014102100,a.example.com,CN,100",
                    "2014102100,b.example.com,CN,50",
                    "2014102100,c.example.com,CN,200",
                    "2014102100,d.example.com,US,250",
                    "2014102100,e.example.com,US,123",
                    "2014102100,f.example.com,US,567",
                    "2014102100,g.example.com,US,11",
                    "2014102100,h.example.com,US,251",
                    "2014102100,i.example.com,US,963",
                    "2014102100,j.example.com,US,333",
                    "2014102200,a.example.com,CN,100",
                    "2014102200,b.example.com,CN,50",
                    "2014102200,c.example.com,CN,200",
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
                true,
                NO_TARGET_ROWS_PER_SEGMENT,
                1000,
                List.of("2014-10-22T00:00:00Z/P1D"),
                1,
                new int[]{1},
                new String[][][]{
                    {
                        {null, null}
                    }
                },
                ImmutableList.of(
                    "2014102200,a.example.com,CN,100",
                    "2014102200,b.example.com,US,50",
                    "2014102200,c.example.com,US,200",
                    "2014102200,d.example.com,US,250",
                    "2014102200,e.example.com,US,123",
                    "2014102200,f.example.com,US,567",
                    "2014102200,g.example.com,US,11",
                    "2014102200,h.example.com,US,251",
                    "2014102200,i.example.com,US,963",
                    "2014102200,j.example.com,US,333"
                )
            }
        }
    );
  }

  public DeterminePartitionsJobTest(
      boolean assumeGrouped,
      @Nullable Integer targetRowsPerSegment,
      Integer maxRowsPerSegment,
      List<String> intervals,
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
    tmpDir = FileUtils.createTempDir();
    tmpDir.deleteOnExit();

    org.apache.commons.io.FileUtils.writeLines(dataFile, data);

    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            DataSchema.builder()
                      .withDataSource("website")
                      .withParserMap(
                          HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                              new StringInputRowParser(
                                  new CSVParseSpec(
                                      new TimestampSpec("timestamp", "yyyyMMddHH", null),
                                      new DimensionsSpec(
                                          DimensionsSpec.getDefaultSchemas(ImmutableList.of("host", "country"))
                                      ),
                                      null,
                                      ImmutableList.of("timestamp", "host", "country", "visited_num"),
                                      false,
                                      0
                                  ),
                                  null
                              ),
                              Map.class
                          )
                      )
                      .withAggregators(new LongSumAggregatorFactory("visited_num", "visited_num"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              Granularities.NONE,
                              intervals.stream().map(Intervals::of).collect(ImmutableList.toImmutableList())
                          )
                      )
                      .withObjectMapper(HadoopDruidIndexerConfig.JSON_MAPPER)
                      .build(),
            new HadoopIOConfig(
                ImmutableMap.of(
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
                new SingleDimensionPartitionsSpec(targetRowsPerSegment, maxRowsPerSegment, null, assumeGrouped),
                null,
                null,
                null,
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
                false,
                false,
                null,
                null,
                null,
                null,
                null,
                1
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

    for (Map.Entry<Long, List<HadoopyShardSpec>> entry : config.getSchema()
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
    org.apache.commons.io.FileUtils.forceDelete(dataFile);
    FileUtils.deleteDirectory(tmpDir);
  }
}
