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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IndexTaskTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private final IndexSpec indexSpec;
  private final ObjectMapper jsonMapper;
  private IndexMerger indexMerger;
  private IndexMergerV9 indexMergerV9;
  private IndexIO indexIO;

  public IndexTaskTest()
  {
    indexSpec = new IndexSpec();
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexMerger = testUtils.getTestIndexMerger();
    indexMergerV9 = testUtils.getTestIndexMergerV9();
    indexIO = testUtils.getTestIndexIO();
  }

  @Test
  public void testDeterminePartitions() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    PrintWriter writer = new PrintWriter(tmpFile);
    writer.println("2014-01-01T00:00:10Z,a,1");
    writer.println("2014-01-01T01:00:20Z,b,1");
    writer.println("2014-01-01T02:00:30Z,c,1");
    writer.close();

    IndexTask indexTask = new IndexTask(
        null,
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "test",
                jsonMapper.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec(
                                "ts",
                                "auto",
                                null
                            ),
                            new DimensionsSpec(
                                DimensionsSpec.getDefaultSchemas(Arrays.asList("ts")),
                                Lists.<String>newArrayList(),
                                Lists.<SpatialDimensionSchema>newArrayList()
                            ),
                            null,
                            Arrays.asList("ts", "dim", "val")
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new UniformGranularitySpec(
                    Granularity.DAY,
                    QueryGranularities.MINUTE,
                    Arrays.asList(new Interval("2014/2015"))
                ),
                jsonMapper
            ),
            new IndexTask.IndexIOConfig(
                new LocalFirehoseFactory(
                    tmpDir,
                    "druid*",
                    null
                )
            ),
            new IndexTask.IndexTuningConfig(
                2,
                0,
                null,
                indexSpec,
                null,
                false
            )
        ),
        jsonMapper,
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(HashBasedNumberedShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(0).getShardSpec()).getPartitions());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(1).getInterval());
    Assert.assertTrue(segments.get(1).getShardSpec().getClass().equals(HashBasedNumberedShardSpec.class));
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(1).getShardSpec()).getPartitions());
  }

  @Test
  public void testForceExtendableShardSpecs() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    PrintWriter writer = new PrintWriter(tmpFile);
    writer.println("2014-01-01T00:00:10Z,a,1");
    writer.println("2014-01-01T01:00:20Z,b,1");
    writer.println("2014-01-01T02:00:30Z,c,1");
    writer.close();

    IndexTask indexTask = new IndexTask(
        null,
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "test",
                jsonMapper.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec(
                                "ts",
                                "auto",
                                null
                            ),
                            new DimensionsSpec(
                                DimensionsSpec.getDefaultSchemas(Arrays.asList("ts")),
                                Lists.<String>newArrayList(),
                                Lists.<SpatialDimensionSchema>newArrayList()
                            ),
                            null,
                            Arrays.asList("ts", "dim", "val")
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new UniformGranularitySpec(
                    Granularity.DAY,
                    QueryGranularities.MINUTE,
                    Arrays.asList(new Interval("2014/2015"))
                ),
                jsonMapper
            ),
            new IndexTask.IndexIOConfig(
                new LocalFirehoseFactory(
                    tmpDir,
                    "druid*",
                    null
                )
            ),
            new IndexTask.IndexTuningConfig(
                2,
                0,
                null,
                indexSpec,
                null,
                true
            )
        ),
        jsonMapper,
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(2, segments.size());

    Assert.assertEquals("test", segments.get(0).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(0).getInterval());
    Assert.assertTrue(segments.get(0).getShardSpec().getClass().equals(NumberedShardSpec.class));
    Assert.assertEquals(0, segments.get(0).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(0).getShardSpec()).getPartitions());

    Assert.assertEquals("test", segments.get(1).getDataSource());
    Assert.assertEquals(new Interval("2014/P1D"), segments.get(1).getInterval());
    Assert.assertTrue(segments.get(1).getShardSpec().getClass().equals(NumberedShardSpec.class));
    Assert.assertEquals(1, segments.get(1).getShardSpec().getPartitionNum());
    Assert.assertEquals(2, ((NumberedShardSpec) segments.get(1).getShardSpec()).getPartitions());
  }

  @Test
  public void testWithArbitraryGranularity() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    PrintWriter writer = new PrintWriter(tmpFile);
    writer.println("2014-01-01T00:00:10Z,a,1");
    writer.println("2014-01-01T01:00:20Z,b,1");
    writer.println("2014-01-01T02:00:30Z,c,1");
    writer.close();

    IndexTask indexTask = new IndexTask(
        null,
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "test",
                jsonMapper.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec(
                                "ts",
                                "auto",
                                null
                            ),
                            new DimensionsSpec(
                                DimensionsSpec.getDefaultSchemas(Arrays.asList("ts")),
                                Lists.<String>newArrayList(),
                                Lists.<SpatialDimensionSchema>newArrayList()
                            ),
                            null,
                            Arrays.asList("ts", "dim", "val")
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new ArbitraryGranularitySpec(
                    QueryGranularities.MINUTE,
                    Arrays.asList(new Interval("2014/2015"))
                ),
                jsonMapper
            ),
            new IndexTask.IndexIOConfig(
                new LocalFirehoseFactory(
                    tmpDir,
                    "druid*",
                    null
                )
            ),
            null
        ),
        jsonMapper,
        null
    );

    List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());
  }

  private final List<DataSegment> runTask(final IndexTask indexTask) throws Exception
  {
    final List<DataSegment> segments = Lists.newArrayList();

    indexTask.run(
        new TaskToolbox(
            null, null, new TaskActionClient()
        {
          @Override
          public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
          {
            if (taskAction instanceof LockListAction) {
              return (RetType) Arrays.asList(
                  new TaskLock(
                      "", "", null, new DateTime().toString()
                  )
              );
            }
            return null;
          }
        }, null, new DataSegmentPusher()
        {
          @Deprecated
          @Override
          public String getPathForHadoop(String dataSource)
          {
            return getPathForHadoop();
          }

          @Override
          public String getPathForHadoop()
          {
            return null;
          }

          @Override
          public DataSegment push(File file, DataSegment segment) throws IOException
          {
            segments.add(segment);
            return segment;
          }
        }, null, null, null, null, null, null, null, null, null, null, temporaryFolder.newFolder(),
            indexMerger, indexIO, null, null, indexMergerV9
        )
    );

    Collections.sort(segments);

    return segments;
  }

  @Test
  public void testIntervalBucketing() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();

    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    PrintWriter writer = new PrintWriter(tmpFile);
    writer.println("2015-03-01T07:59:59.977Z,a,1");
    writer.println("2015-03-01T08:00:00.000Z,b,1");
    writer.close();

    IndexTask indexTask = new IndexTask(
        null,
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "test",
                jsonMapper.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new TimestampSpec(
                                "ts",
                                "auto",
                                null
                            ),
                            new DimensionsSpec(
                                DimensionsSpec.getDefaultSchemas(Arrays.asList("dim")),
                                Lists.<String>newArrayList(),
                                Lists.<SpatialDimensionSchema>newArrayList()
                            ),
                            null,
                            Arrays.asList("ts", "dim", "val")
                        ),
                        null
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{
                    new LongSumAggregatorFactory("val", "val")
                },
                new UniformGranularitySpec(
                    Granularity.HOUR,
                    QueryGranularities.HOUR,
                    Arrays.asList(new Interval("2015-03-01T08:00:00Z/2015-03-01T09:00:00Z"))
                ),
                jsonMapper
            ),
            new IndexTask.IndexIOConfig(
                new LocalFirehoseFactory(
                    tmpDir,
                    "druid*",
                    null
                )
            ),
            null
        ),
        jsonMapper,
        null
    );

    final List<DataSegment> segments = runTask(indexTask);

    Assert.assertEquals(1, segments.size());
  }

  @Test
  public void testConvertProps()
  {
    ShardSpec spec = new NumberedShardSpec(1, 2);
    IndexTask.IndexTuningConfig config = new IndexTask.IndexTuningConfig(
        100,
        1000,
        null,
        new IndexSpec(),
        null,
        false
    );
    RealtimeTuningConfig realtimeTuningConfig = IndexTask.convertTuningConfig(
        spec,
        config.getRowFlushBoundary(),
        config.getIndexSpec(),
        config.getBuildV9Directly()
    );
    Assert.assertEquals(realtimeTuningConfig.getMaxRowsInMemory(), config.getRowFlushBoundary());
    Assert.assertEquals(realtimeTuningConfig.getShardSpec(), spec);
    Assert.assertEquals(realtimeTuningConfig.getIndexSpec(), indexSpec);
  }

}
