package com.metamx.druid.indexing.common.task;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.CountAggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexGranularity;
import com.metamx.druid.indexer.HadoopDruidIndexerConfig;
import com.metamx.druid.indexer.data.JSONDataSpec;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.indexer.path.StaticPathSpec;
import com.metamx.druid.indexer.rollup.DataRollupSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.realtime.Schema;
import com.metamx.druid.shard.NoneShardSpec;
import junit.framework.Assert;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Test;

public class TaskSerdeTest
{
  @Test
  public void testIndexTaskSerde() throws Exception
  {
    final Task task = new IndexTask(
        null,
        "foo",
        new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P2D"))),
        null,
        new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
        QueryGranularity.NONE,
        10000,
        null,
        -1
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P2D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
  }

  @Test
  public void testIndexGeneratorTaskSerde() throws Exception
  {
    final Task task = new IndexGeneratorTask(
        null,
        "foo",
        new Interval("2010-01-01/P1D"),
        null,
        new Schema(
            "foo",
            null,
            new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
            QueryGranularity.NONE,
            new NoneShardSpec()
        ),
        -1
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
  }

  @Test
  public void testMergeTaskSerde() throws Exception
  {
    final Task task = new MergeTask(
        null,
        "foo",
        ImmutableList.<DataSegment>of(
            DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
        ),
        ImmutableList.<AggregatorFactory>of(
            new CountAggregatorFactory("cnt")
        )
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(((MergeTask) task).getSegments(), ((MergeTask) task2).getSegments());
    Assert.assertEquals(
        ((MergeTask) task).getAggregators().get(0).getName(),
        ((MergeTask) task2).getAggregators().get(0).getName()
    );
  }

  @Test
  public void testKillTaskSerde() throws Exception
  {
    final Task task = new KillTask(
        null,
        "foo",
        new Interval("2010-01-01/P1D")
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
  }

  @Test
  public void testVersionConverterTaskSerde() throws Exception
  {
    final Task task = VersionConverterTask.create(
        DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(((VersionConverterTask) task).getSegment(), ((VersionConverterTask) task).getSegment());
  }

  @Test
  public void testVersionConverterSubTaskSerde() throws Exception
  {
    final Task task = new VersionConverterTask.SubTask(
        "myGroupId",
        DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());
    Assert.assertEquals("myGroupId", task.getGroupId());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(
        ((VersionConverterTask.SubTask) task).getSegment(),
        ((VersionConverterTask.SubTask) task).getSegment()
    );
  }

  @Test
  public void testRealtimeIndexTaskSerde() throws Exception
  {
    final Task task = new RealtimeIndexTask(
        null,
        null,
        new Schema("foo", null, new AggregatorFactory[0], QueryGranularity.NONE, new NoneShardSpec()),
        null,
        null,
        new Period("PT10M"),
        IndexGranularity.HOUR
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.<Interval>absent(), task.getImplicitLockInterval());
    Assert.assertEquals(new Period("PT10M"), ((RealtimeIndexTask) task).getWindowPeriod());
    Assert.assertEquals(IndexGranularity.HOUR, ((RealtimeIndexTask) task).getSegmentGranularity());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(((RealtimeIndexTask) task).getWindowPeriod(), ((RealtimeIndexTask) task).getWindowPeriod());
    Assert.assertEquals(
        ((RealtimeIndexTask) task).getSegmentGranularity(),
        ((RealtimeIndexTask) task).getSegmentGranularity()
    );
  }

  @Test
  public void testDeleteTaskSerde() throws Exception
  {
    final Task task = new DeleteTask(
        null,
        "foo",
        new Interval("2010-01-01/P1D")
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(task.getImplicitLockInterval().get(), task2.getImplicitLockInterval().get());
  }

  @Test
  public void testDeleteTaskFromJson() throws Exception
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final Task task = jsonMapper.readValue(
        "{\"type\":\"delete\",\"dataSource\":\"foo\",\"interval\":\"2010-01-01/P1D\"}",
        Task.class
    );
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertNotNull(task.getId());
    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(task.getImplicitLockInterval().get(), task2.getImplicitLockInterval().get());
  }

  @Test
  public void testAppendTaskSerde() throws Exception
  {
    final Task task = new AppendTask(
        null,
        "foo",
        ImmutableList.of(
            DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
        )
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);

    Thread.sleep(100); // Just want to run the clock a bit to make sure the task id doesn't change
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
    Assert.assertEquals(task.getImplicitLockInterval().get(), task2.getImplicitLockInterval().get());
    Assert.assertEquals(((AppendTask) task).getSegments(), ((AppendTask) task2).getSegments());
  }

  @Test
  public void testHadoopIndexTaskSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopDruidIndexerConfig(
            null,
            "foo",
            "timestamp",
            "auto",
            new JSONDataSpec(ImmutableList.of("foo"), null),
            null,
            new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P1D"))),
            new StaticPathSpec("bar"),
            null,
            null,
            null,
            null,
            null,
            null,
            false,
            true,
            null,
            false,
            new DataRollupSpec(ImmutableList.<AggregatorFactory>of(), QueryGranularity.NONE),
            null,
            false,
            ImmutableList.<String>of()
        )
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getImplicitLockInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getImplicitLockInterval(), task2.getImplicitLockInterval());
  }
}
