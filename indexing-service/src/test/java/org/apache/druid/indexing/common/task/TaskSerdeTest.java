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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;

public class TaskSerdeTest
{
  private final ObjectMapper jsonMapper;
  private final IndexSpec indexSpec = IndexSpec.getDefault();

  public TaskSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    jsonMapper.registerSubtypes(
        new NamedType(ParallelIndexTuningConfig.class, "index_parallel"),
        new NamedType(IndexTuningConfig.class, "index")
    );
  }

  @Test
  public void testIndexTaskIOConfigDefaults() throws Exception
  {
    final IndexTask.IndexIOConfig ioConfig = jsonMapper.readValue(
        "{\"type\":\"index\",\"inputSource\":{\"type\":\"noop\"},\"inputFormat\":{\"type\":\"noop\"}}",
        IndexTask.IndexIOConfig.class
    );

    Assertions.assertEquals(false, ioConfig.isAppendToExisting());
    Assertions.assertEquals(false, ioConfig.isDropExisting());
  }

  @Test
  public void testIndexTaskTuningConfigDefaults() throws Exception
  {
    final IndexTask.IndexTuningConfig tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertFalse(tuningConfig.isReportParseExceptions());
    Assertions.assertEquals(IndexSpec.getDefault(), tuningConfig.getIndexSpec());
    Assertions.assertEquals(new Period(Integer.MAX_VALUE), tuningConfig.getIntermediatePersistPeriod());
    Assertions.assertEquals(0, tuningConfig.getMaxPendingPersists());
    Assertions.assertEquals(1000000, tuningConfig.getMaxRowsInMemory());
    Assertions.assertNull(getNumShards(tuningConfig));
    Assertions.assertNull(getMaxRowsPerSegment(tuningConfig));
  }

  @Test
  public void testIndexTaskTuningConfigTargetPartitionSizeOrNumShards() throws Exception
  {
    IndexTask.IndexTuningConfig tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertEquals(10, (int) getMaxRowsPerSegment(tuningConfig));
    Assertions.assertNull(getNumShards(tuningConfig));

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\"}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertNull(getMaxRowsPerSegment(tuningConfig));

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"maxRowsPerSegment\":10}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertEquals(10, (int) getMaxRowsPerSegment(tuningConfig));
    Assertions.assertNull(getNumShards(tuningConfig));

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"numShards\":10, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertNull(getMaxRowsPerSegment(tuningConfig));
    Assertions.assertEquals(10, (int) getNumShards(tuningConfig));

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":-1, \"numShards\":10, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertNull(getMaxRowsPerSegment(tuningConfig));
    Assertions.assertEquals(10, (int) getNumShards(tuningConfig));

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":10, \"numShards\":-1}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertNull(getNumShards(tuningConfig));
    Assertions.assertEquals(10, (int) getMaxRowsPerSegment(tuningConfig));

    tuningConfig = jsonMapper.readValue(
        "{\"type\":\"index\", \"targetPartitionSize\":-1, \"numShards\":-1, \"forceGuaranteedRollup\": true}",
        IndexTask.IndexTuningConfig.class
    );

    Assertions.assertNull(getNumShards(tuningConfig));
    Assertions.assertNotNull(getMaxRowsPerSegment(tuningConfig));
    Assertions.assertEquals(PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT, getMaxRowsPerSegment(tuningConfig).intValue());
  }

  @Test
  public void testIndexTaskTuningConfigTargetPartitionSizeAndNumShards() throws Exception
  {
    final Exception exception = Assertions.assertThrows(
        Exception.class,
        () -> jsonMapper.readValue(
            "{\"type\":\"index\", \"targetPartitionSize\":10, \"numShards\":10, \"forceGuaranteedRollup\": true}",
            IndexTask.IndexTuningConfig.class
        )
    );
    Assertions.assertInstanceOf(IllegalArgumentException.class, exception.getCause());
  }

  @Test
  public void testTaskResourceValid() throws Exception
  {
    TaskResource actual = jsonMapper.readValue(
        "{\"availabilityGroup\":\"index_xxx_mmm\", \"requiredCapacity\":1}",
        TaskResource.class
    );
    Assertions.assertNotNull(actual);
    Assertions.assertNotNull(actual.getAvailabilityGroup());
    Assertions.assertTrue(actual.getRequiredCapacity() > 0);
  }

  @Test
  public void testTaskResourceWithNullAvailabilityGroupShouldFail() throws Exception
  {
    final Exception exception = Assertions.assertThrows(
        Exception.class,
        () -> jsonMapper.readValue(
            "{\"availabilityGroup\":null, \"requiredCapacity\":10}",
            TaskResource.class
        )
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
  }

  @Test
  public void testTaskResourceWithZeroRequiredCapacityShouldFail() throws Exception
  {
    final Exception exception = Assertions.assertThrows(
        Exception.class,
        () -> jsonMapper.readValue(
            "{\"availabilityGroup\":null, \"requiredCapacity\":0}",
            TaskResource.class
        )
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
  }

  @Test
  public void testTaskResourceWithNegativeRequiredCapacityShouldFail() throws Exception
  {
    final Exception exception = Assertions.assertThrows(
        Exception.class,
        () -> jsonMapper.readValue(
            "{\"availabilityGroup\":null, \"requiredCapacity\":-1}",
            TaskResource.class
        )
    );
    Assertions.assertInstanceOf(NullPointerException.class, exception.getCause());
  }

  @Test
  public void testIndexTaskSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(TimestampSpec.DEFAULT)
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new LocalInputSource(new File("lol"), "rofl"), new NoopInputFormat(), true, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsInMemory(10)
                               .withPartitionsSpec(new DynamicPartitionsSpec(10000, null))
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .withForceGuaranteedRollup(false)
                               .withAwaitSegmentAvailabilityTimeoutMillis(1L)
                               .build()
        ),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = (IndexTask) jsonMapper.readValue(json, Task.class);

    Assertions.assertEquals("foo", task.getDataSource());

    Assertions.assertEquals(task.getId(), task2.getId());
    Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    Assertions.assertEquals(task.getDataSource(), task2.getDataSource());

    IndexTask.IndexIOConfig taskIoConfig = task.getIngestionSchema().getIOConfig();
    IndexTask.IndexIOConfig task2IoConfig = task2.getIngestionSchema().getIOConfig();

    Assertions.assertTrue(taskIoConfig.getInputSource() instanceof LocalInputSource);
    Assertions.assertTrue(task2IoConfig.getInputSource() instanceof LocalInputSource);
    Assertions.assertEquals(taskIoConfig.isAppendToExisting(), task2IoConfig.isAppendToExisting());
    Assertions.assertEquals(taskIoConfig.isDropExisting(), task2IoConfig.isDropExisting());

    IndexTask.IndexTuningConfig taskTuningConfig = task.getIngestionSchema().getTuningConfig();
    IndexTask.IndexTuningConfig task2TuningConfig = task2.getIngestionSchema().getTuningConfig();

    Assertions.assertEquals(taskTuningConfig.getBasePersistDirectory(), task2TuningConfig.getBasePersistDirectory());
    Assertions.assertEquals(taskTuningConfig.getIndexSpec(), task2TuningConfig.getIndexSpec());
    Assertions.assertEquals(
        taskTuningConfig.getIntermediatePersistPeriod(),
        task2TuningConfig.getIntermediatePersistPeriod()
    );
    Assertions.assertEquals(taskTuningConfig.getMaxPendingPersists(), task2TuningConfig.getMaxPendingPersists());
    Assertions.assertEquals(taskTuningConfig.getMaxRowsInMemory(), task2TuningConfig.getMaxRowsInMemory());
    Assertions.assertEquals(getNumShards(taskTuningConfig), getNumShards(task2TuningConfig));
    Assertions.assertEquals(getMaxRowsPerSegment(taskTuningConfig), getMaxRowsPerSegment(task2TuningConfig));
    Assertions.assertEquals(taskTuningConfig.isReportParseExceptions(), task2TuningConfig.isReportParseExceptions());
    Assertions.assertEquals(taskTuningConfig.getAwaitSegmentAvailabilityTimeoutMillis(), task2TuningConfig.getAwaitSegmentAvailabilityTimeoutMillis());
  }

  @Test
  public void testIndexTaskwithResourceSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        new TaskResource("rofl", 2),
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(TimestampSpec.DEFAULT)
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new LocalInputSource(new File("lol"), "rofl"), new NoopInputFormat(), true, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsInMemory(10)
                               .withForceGuaranteedRollup(false)
                               .withPartitionsSpec(new DynamicPartitionsSpec(10000, null))
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .build()
        ),
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = (IndexTask) jsonMapper.readValue(json, Task.class);

    Assertions.assertEquals("foo", task.getDataSource());

    Assertions.assertEquals(task.getId(), task2.getId());
    Assertions.assertEquals(2, task.getTaskResource().getRequiredCapacity());
    Assertions.assertEquals("rofl", task.getTaskResource().getAvailabilityGroup());
    Assertions.assertEquals(task.getTaskResource().getRequiredCapacity(), task2.getTaskResource().getRequiredCapacity());
    Assertions.assertEquals(task.getTaskResource().getAvailabilityGroup(), task2.getTaskResource().getAvailabilityGroup());
    Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    Assertions.assertTrue(task.getIngestionSchema().getIOConfig().getInputSource() instanceof LocalInputSource);
    Assertions.assertTrue(task2.getIngestionSchema().getIOConfig().getInputSource() instanceof LocalInputSource);
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

    Assertions.assertEquals("foo", task.getDataSource());
    Assertions.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assertions.assertEquals(task.getId(), task2.getId());
    Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    Assertions.assertEquals(task.getInterval(), task2.getInterval());
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

    Assertions.assertEquals("foo", task.getDataSource());
    Assertions.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());

    Assertions.assertEquals(task.getId(), task2.getId());
    Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    Assertions.assertEquals(task.getInterval(), task2.getInterval());
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

    Assertions.assertEquals("foo", task.getDataSource());
    Assertions.assertEquals(Intervals.of("2010-01-01/P1D"), task.getInterval());
    Assertions.assertEquals(ImmutableMap.<String, Object>of("bucket", "hey", "baseKey", "what"), task.getTargetLoadSpec());

    Assertions.assertEquals(task.getId(), task2.getId());
    Assertions.assertEquals(task.getGroupId(), task2.getGroupId());
    Assertions.assertEquals(task.getDataSource(), task2.getDataSource());
    Assertions.assertEquals(task.getInterval(), task2.getInterval());
    Assertions.assertEquals(task.getTargetLoadSpec(), task2.getTargetLoadSpec());
  }

  private static Integer getMaxRowsPerSegment(final IndexTask.IndexTuningConfig tuningConfig)
  {
    final PartitionsSpec partitionsSpec = tuningConfig.getPartitionsSpec();
    return partitionsSpec == null ? null : partitionsSpec.getMaxRowsPerSegment();
  }

  private static Integer getNumShards(final IndexTask.IndexTuningConfig tuningConfig)
  {
    final PartitionsSpec partitionsSpec = tuningConfig.getPartitionsSpec();
    return partitionsSpec instanceof HashedPartitionsSpec
           ? ((HashedPartitionsSpec) partitionsSpec).getNumShards()
           : null;
  }
}
