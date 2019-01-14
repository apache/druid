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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.indexing.ClientAppendQuery;
import org.apache.druid.client.indexing.ClientKillQuery;
import org.apache.druid.client.indexing.ClientMergeQuery;
import org.apache.druid.guice.FirehoseModule;
import org.apache.druid.indexer.HadoopIOConfig;
import org.apache.druid.indexer.HadoopIngestionSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.RealtimeIOConfig;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.FireDepartment;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.util.List;

public class TaskSerdeTest
{
  private final ObjectMapper jsonMapper;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final IndexSpec indexSpec = new IndexSpec();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public TaskSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    rowIngestionMetersFactory = testUtils.getRowIngestionMetersFactory();

    for (final Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testIndexTaskIOConfigDefaults() throws Exception
  {
    final IndexTask.IndexIOConfig ioConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexIOConfig.class
    );

    Assert.assertEquals(false, ioConfig.isAppendToExisting());
  }

  @Test
  public void testIndexTaskTuningConfigDefaults() throws Exception
  {
    final IndexTask.IndexTuningConfig tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertFalse(tuningConfig.isForceExtendableShardSpecs());
    Assert.assertFalse(tuningConfig.isReportParseExceptions());
    Assert.assertEquals(new IndexSpec(), tuningConfig.getIndexSpec());
    Assert.assertEquals(new Period(Integer.MAX_VALUE), tuningConfig.getIntermediatePersistPeriod());
    Assert.assertEquals(0, tuningConfig.getMaxPendingPersists());
    Assert.assertEquals(1000000, tuningConfig.getMaxRowsInMemory());
    Assert.assertNull(tuningConfig.getNumShards());
    Assert.assertNull(tuningConfig.getMaxRowsPerSegment());
  }

  @Test
  public void testIndexTaskTuningConfigTargetPartitionSizeOrNumShards() throws Exception
  {
    IndexTask.IndexTuningConfig tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertEquals(10, (int) tuningConfig.getMaxRowsPerSegment());
    Assert.assertNull(tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertNull(tuningConfig.getMaxRowsPerSegment());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"maxRowsPerSegment\":10}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertEquals(10, (int) tuningConfig.getMaxRowsPerSegment());
    Assert.assertNull(tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"numShards\":10}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertNull(tuningConfig.getMaxRowsPerSegment());
    Assert.assertEquals(10, (int) tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":-1, \"numShards\":10}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertNull(tuningConfig.getMaxRowsPerSegment());
    Assert.assertEquals(10, (int) tuningConfig.getNumShards());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10, \"numShards\":-1}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertNull(tuningConfig.getNumShards());
    Assert.assertEquals(10, (int) tuningConfig.getMaxRowsPerSegment());

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":-1, \"numShards\":-1}",
        IndexTask.IndexTuningConfig.class
    );

    Assert.assertNull(tuningConfig.getNumShards());
    Assert.assertNull(tuningConfig.getMaxRowsPerSegment());
  }

  @Test
  public void testIndexTaskTuningConfigTargetPartitionSizeAndNumShards() throws Exception
  {
    thrown.expectCause(CoreMatchers.isA(IllegalArgumentException.class));

    jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10, \"numShards\":10}",
        IndexTask.IndexTuningConfig.class
    );
  }

  @Test
  public void testIndexTaskSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    null,
                    ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                ),
                null,
                jsonMapper
            ),
            new IndexIOConfig(new LocalFirehoseFactory(new File("lol"), "rofl", null), true),
            new IndexTuningConfig(
                null,
                10000,
                10,
                null,
                null,
                9999,
                null,
                null,
                indexSpec,
                3,
                true,
                true,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = (IndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());

    IndexTask.IndexIOConfig taskIoConfig = task.getIngestionSchema().getIOConfig();
    IndexTask.IndexIOConfig task2IoConfig = task2.getIngestionSchema().getIOConfig();

    Assert.assertTrue(taskIoConfig.getFirehoseFactory() instanceof LocalFirehoseFactory);
    Assert.assertTrue(task2IoConfig.getFirehoseFactory() instanceof LocalFirehoseFactory);
    Assert.assertEquals(taskIoConfig.isAppendToExisting(), task2IoConfig.isAppendToExisting());

    IndexTask.IndexTuningConfig taskTuningConfig = task.getIngestionSchema().getTuningConfig();
    IndexTask.IndexTuningConfig task2TuningConfig = task2.getIngestionSchema().getTuningConfig();

    Assert.assertEquals(taskTuningConfig.getBasePersistDirectory(), task2TuningConfig.getBasePersistDirectory());
    Assert.assertEquals(taskTuningConfig.getIndexSpec(), task2TuningConfig.getIndexSpec());
    Assert.assertEquals(
        taskTuningConfig.getIntermediatePersistPeriod(),
        task2TuningConfig.getIntermediatePersistPeriod()
    );
    Assert.assertEquals(taskTuningConfig.getMaxPendingPersists(), task2TuningConfig.getMaxPendingPersists());
    Assert.assertEquals(taskTuningConfig.getMaxRowsInMemory(), task2TuningConfig.getMaxRowsInMemory());
    Assert.assertEquals(taskTuningConfig.getNumShards(), task2TuningConfig.getNumShards());
    Assert.assertEquals(taskTuningConfig.getMaxRowsPerSegment(), task2TuningConfig.getMaxRowsPerSegment());
    Assert.assertEquals(
        taskTuningConfig.isForceExtendableShardSpecs(),
        task2TuningConfig.isForceExtendableShardSpecs()
    );
    Assert.assertEquals(taskTuningConfig.isReportParseExceptions(), task2TuningConfig.isReportParseExceptions());
  }

  @Test
  public void testIndexTaskwithResourceSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        new TaskResource("rofl", 2),
        new IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    null,
                    ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                ),
                null,
                jsonMapper
            ),
            new IndexIOConfig(new LocalFirehoseFactory(new File("lol"), "rofl", null), true),
            new IndexTuningConfig(
                null,
                10000,
                10,
                null,
                null,
                null,
                null,
                null,
                indexSpec,
                3,
                true,
                true,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null,
        rowIngestionMetersFactory
    );

    for (final Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = (IndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(2, task.getTaskResource().getRequiredCapacity());
    Assert.assertEquals("rofl", task.getTaskResource().getAvailabilityGroup());
    Assert.assertEquals(task.getTaskResource().getRequiredCapacity(), task2.getTaskResource().getRequiredCapacity());
    Assert.assertEquals(task.getTaskResource().getAvailabilityGroup(), task2.getTaskResource().getAvailabilityGroup());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertTrue(task.getIngestionSchema().getIOConfig().getFirehoseFactory() instanceof LocalFirehoseFactory);
    Assert.assertTrue(task2.getIngestionSchema().getIOConfig().getFirehoseFactory() instanceof LocalFirehoseFactory);
  }

  @Test
  public void testMergeTaskSerde() throws Exception
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(Intervals.of("2010-01-01/P1D"))
                   .version("1234")
                   .build()
    );
    final List<AggregatorFactory> aggregators = ImmutableList.of(new CountAggregatorFactory("cnt"));
    final MergeTask task = new MergeTask(
        null,
        "foo",
        segments,
        aggregators,
        true,
        indexSpec,
        true,
        null,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final MergeTask task2 = (MergeTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getSegments(), task2.getSegments());
    Assert.assertEquals(
        task.getAggregators().get(0).getName(),
        task2.getAggregators().get(0).getName()
    );

    final MergeTask task3 = (MergeTask) jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            new ClientMergeQuery(
                "foo",
                segments,
                aggregators
            )
        ), Task.class
    );

    Assert.assertEquals("foo", task3.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task3.getInterval());
    Assert.assertEquals(segments, task3.getSegments());
    Assert.assertEquals(aggregators, task3.getAggregators());
  }

  @Test
  public void testSameIntervalMergeTaskSerde() throws Exception
  {
    final List<AggregatorFactory> aggregators = ImmutableList.of(new CountAggregatorFactory("cnt"));
    final SameIntervalMergeTask task = new SameIntervalMergeTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        aggregators,
        true,
        indexSpec,
        true,
        null,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final SameIntervalMergeTask task2 = (SameIntervalMergeTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getRollup(), task2.getRollup());
    Assert.assertEquals(task.getIndexSpec(), task2.getIndexSpec());
    Assert.assertEquals(
        task.getAggregators().get(0).getName(),
        task2.getAggregators().get(0).getName()
    );
  }

  @Test
  public void testKillTaskSerde() throws Exception
  {
    final KillTask task = new KillTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final KillTask task2 = (KillTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());

    final KillTask task3 = (KillTask) jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            new ClientKillQuery(
                "foo",
                Intervals.of("2010-01-01/P1D")
            )
        ), Task.class
    );

    Assert.assertEquals("foo", task3.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task3.getInterval());
  }

  @Test
  public void testRealtimeIndexTaskSerde() throws Exception
  {

    final RealtimeIndexTask task = new RealtimeIndexTask(
        null,
        new TaskResource("rofl", 2),
        new FireDepartment(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[0],
                new UniformGranularitySpec(Granularities.HOUR, Granularities.NONE, null),
                null,
                jsonMapper
            ),
            new RealtimeIOConfig(
                new LocalFirehoseFactory(new File("lol"), "rofl", null),
                (schema, config, metrics) -> null,
                null
            ),

            new RealtimeTuningConfig(
                1,
                null,
                new Period("PT10M"),
                null,
                null,
                null,
                null,
                1,
                NoneShardSpec.instance(),
                indexSpec,
                null,
                0,
                0,
                true,
                null,
                null,
                null,
                null
            )
        ),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final RealtimeIndexTask task2 = (RealtimeIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(2, task.getTaskResource().getRequiredCapacity());
    Assert.assertEquals("rofl", task.getTaskResource().getAvailabilityGroup());
    Assert.assertEquals(
        new Period("PT10M"),
        task.getRealtimeIngestionSchema()
            .getTuningConfig().getWindowPeriod()
    );
    Assert.assertEquals(
        Granularities.HOUR,
        task.getRealtimeIngestionSchema().getDataSchema().getGranularitySpec().getSegmentGranularity()
    );
    Assert.assertTrue(task.getRealtimeIngestionSchema().getTuningConfig().isReportParseExceptions());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getTaskResource().getRequiredCapacity(), task2.getTaskResource().getRequiredCapacity());
    Assert.assertEquals(task.getTaskResource().getAvailabilityGroup(), task2.getTaskResource().getAvailabilityGroup());
    Assert.assertEquals(
        task.getRealtimeIngestionSchema().getTuningConfig().getWindowPeriod(),
        task2.getRealtimeIngestionSchema().getTuningConfig().getWindowPeriod()
    );
    Assert.assertEquals(
        task.getRealtimeIngestionSchema().getDataSchema().getGranularitySpec().getSegmentGranularity(),
        task2.getRealtimeIngestionSchema().getDataSchema().getGranularitySpec().getSegmentGranularity()
    );
  }

  @Test
  public void testAppendTaskSerde() throws Exception
  {
    final List<DataSegment> segments = ImmutableList.of(
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(Intervals.of("2010-01-01/P1D"))
                   .version("1234")
                   .build(),
        DataSegment.builder()
                   .dataSource("foo")
                   .interval(Intervals.of("2010-01-02/P1D"))
                   .version("5678")
                   .build()
    );
    final AppendTask task = new AppendTask(
        null,
        "foo",
        segments,
        ImmutableList.of(
            new CountAggregatorFactory("cnt")
        ),
        indexSpec,
        true,
        null,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final AppendTask task2 = (AppendTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P2D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getSegments(), task2.getSegments());

    final AppendTask task3 = (AppendTask) jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            new ClientAppendQuery(
                "foo",
                segments
            )
        ), Task.class
    );

    Assert.assertEquals("foo", task3.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P2D"), task3.getInterval());
    Assert.assertEquals(task3.getSegments(), segments);
    Assert.assertEquals(task.getAggregators(), task2.getAggregators());
  }

  @Test
  public void testArchiveTaskSerde() throws Exception
  {
    final ArchiveTask task = new ArchiveTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final ArchiveTask task2 = (ArchiveTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
  }

  @Test
  public void testRestoreTaskSerde() throws Exception
  {
    final RestoreTask task = new RestoreTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final RestoreTask task2 = (RestoreTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
  }

  @Test
  public void testMoveTaskSerde() throws Exception
  {
    final MoveTask task = new MoveTask(
        null,
        "foo",
        Intervals.of("2010-01-01/P1D"),
        ImmutableMap.of("bucket", "hey", "baseKey", "what"),
        null,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final MoveTask task2 = (MoveTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());
    Assert.assertEquals(ImmutableMap.<String, Object>of("bucket", "hey", "baseKey", "what"), task.getTargetLoadSpec());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getTargetLoadSpec(), task2.getTargetLoadSpec());
  }

  @Test
  public void testHadoopIndexTaskSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            new DataSchema(
                "foo", null, new AggregatorFactory[0], new UniformGranularitySpec(
                Granularities.DAY,
                null,
                ImmutableList.of(Intervals.of("2010-01-01/P1D"))
            ),
                null,
                jsonMapper
            ), new HadoopIOConfig(ImmutableMap.of("paths", "bar"), null, null), null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    final HadoopIndexTask task2 = (HadoopIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(
        task.getSpec().getTuningConfig().getJobProperties(),
        task2.getSpec().getTuningConfig().getJobProperties()
    );
    Assert.assertEquals("blah", task.getClasspathPrefix());
    Assert.assertEquals("blah", task2.getClasspathPrefix());
  }
}
