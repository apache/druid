package com.metamx.druid.merger.common.task;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.client.DataSegment;
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
import org.junit.Test;

public class TaskSerdeTest
{
  @Test
  public void testIndexTaskSerde() throws Exception
  {
    final Task task = new IndexTask(
        "foo",
        new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P2D"))),
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
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P2D")), task.getFixedInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getFixedInterval(), task2.getFixedInterval());
  }

  @Test
  public void testIndexGeneratorTaskSerde() throws Exception
  {
    final Task task = new IndexGeneratorTask(
        "foo",
        new Interval("2010-01-01/P1D"),
        null,
        new Schema(
            "foo",
            new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
            QueryGranularity.NONE,
            new NoneShardSpec()
        ),
        -1
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getFixedInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getFixedInterval(), task2.getFixedInterval());
  }

  @Test
  public void testAppendTaskSerde() throws Exception
  {
    final Task task = new AppendTask(
        "foo",
        ImmutableList.<DataSegment>of(
            DataSegment.builder().dataSource("foo").interval(new Interval("2010-01-01/P1D")).version("1234").build()
        )
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getFixedInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getFixedInterval(), task2.getFixedInterval());
  }

  @Test
  public void testDeleteTaskSerde() throws Exception
  {
    final Task task = new DeleteTask(
        "foo",
        new Interval("2010-01-01/P1D")
    );

    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    final String json = jsonMapper.writeValueAsString(task);
    final Task task2 = jsonMapper.readValue(json, Task.class);

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getFixedInterval(), task2.getFixedInterval());
    Assert.assertEquals(task.getFixedInterval().get(), task2.getFixedInterval().get());
  }

  @Test
  public void testHadoopIndexTaskSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        new HadoopDruidIndexerConfig(
            null,
            "foo",
            "timestamp",
            "auto",
            new JSONDataSpec(ImmutableList.of("foo")),
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
    Assert.assertEquals(Optional.of(new Interval("2010-01-01/P1D")), task.getFixedInterval());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(task.getFixedInterval(), task2.getFixedInterval());
  }
}
