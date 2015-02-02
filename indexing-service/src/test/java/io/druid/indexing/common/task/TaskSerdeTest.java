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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.Granularity;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.FirehoseModule;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeIOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.realtime.FireDepartment;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.PlumberSchool;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import junit.framework.Assert;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;

import java.io.File;

public class TaskSerdeTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testIndexTaskSerde() throws Exception
  {
    final IndexTask task = new IndexTask(
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularity.DAY,
                    null,
                    ImmutableList.of(new Interval("2010-01-01/P2D"))
                )
            ),
            new IndexTask.IndexIOConfig(new LocalFirehoseFactory(new File("lol"), "rofl", null)),
            new IndexTask.IndexTuningConfig(10000, -1, -1)
        ),
        jsonMapper
    );

    for (final Module jacksonModule : new FirehoseModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
    InjectableValues inject = new InjectableValues.Std()
        .addValue(ObjectMapper.class, jsonMapper);
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final IndexTask task2 = jsonMapper.reader(Task.class).with(inject).readValue(json);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P2D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertTrue(task.getIngestionSchema().getIOConfig().getFirehoseFactory() instanceof LocalFirehoseFactory);
    Assert.assertTrue(task2.getIngestionSchema().getIOConfig().getFirehoseFactory() instanceof LocalFirehoseFactory);
  }

  @Test
  public void testMergeTaskSerde() throws Exception
  {
    final MergeTask task = new MergeTask(
        null,
        "foo",
        ImmutableList.<DataSegment>of(
            DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
        ),
        ImmutableList.<AggregatorFactory>of(
            new CountAggregatorFactory("cnt")
        )
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final MergeTask task2 = (MergeTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getSegments(), task2.getSegments());
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
        new Interval("2010-01-01/P1D")
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final KillTask task2 = (KillTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
  }

  @Test
  public void testVersionConverterTaskSerde() throws Exception
  {
    final VersionConverterTask task = VersionConverterTask.create(
        DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final VersionConverterTask task2 = (VersionConverterTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getSegment(), task.getSegment());
  }

  @Test
  public void testVersionConverterSubTaskSerde() throws Exception
  {
    final VersionConverterTask.SubTask task = new VersionConverterTask.SubTask(
        "myGroupId",
        DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final VersionConverterTask.SubTask task2 = (VersionConverterTask.SubTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals("myGroupId", task.getGroupId());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getSegment(), task2.getSegment());
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
                new UniformGranularitySpec(Granularity.HOUR, QueryGranularity.NONE, null)
            ),
            new RealtimeIOConfig(new LocalFirehoseFactory(new File("lol"), "rofl", null), new PlumberSchool()
            {
              @Override
              public Plumber findPlumber(
                  DataSchema schema, RealtimeTuningConfig config, FireDepartmentMetrics metrics
              )
              {
                return null;
              }
            }),
            new RealtimeTuningConfig(
                1,
                new Period("PT10M"),
                null,
                null,
                null,
                null,
                1,
                new NoneShardSpec(),
                false,
                false,
                null
            )
        )
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
        Granularity.HOUR,
        task.getRealtimeIngestionSchema().getDataSchema().getGranularitySpec().getSegmentGranularity()
    );

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
    final AppendTask task = new AppendTask(
        null,
        "foo",
        ImmutableList.of(
            DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
        )
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final AppendTask task2 = (AppendTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getInterval(), task2.getInterval());
    Assert.assertEquals(task.getSegments(), task2.getSegments());
  }

  @Test
  public void testArchiveTaskSerde() throws Exception
  {
    final ArchiveTask task = new ArchiveTask(
        null,
        "foo",
        new Interval("2010-01-01/P1D")
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final ArchiveTask task2 = (ArchiveTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());

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
        new Interval("2010-01-01/P1D")
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final RestoreTask task2 = (RestoreTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());

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
        new Interval("2010-01-01/P1D"),
        ImmutableMap.<String, Object>of("bucket", "hey", "baseKey", "what")
    );

    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final MoveTask task2 = (MoveTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(new Interval("2010-01-01/P1D"), task.getInterval());
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
                Granularity.DAY,
                null,
                ImmutableList.of(new Interval("2010-01-01/P1D"))
            )
            ), new HadoopIOConfig(ImmutableMap.<String, Object>of("paths", "bar"), null, null), null
        ),
        null,
        null,
        "blah"
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
