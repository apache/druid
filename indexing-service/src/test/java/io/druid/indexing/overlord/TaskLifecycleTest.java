/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.guava.Comparators;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.LocalTaskActionClientFactory;
import io.druid.indexing.common.actions.LockListAction;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.actions.TaskActionToolbox;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.AbstractFixedIntervalTask;
import io.druid.indexing.common.task.IndexTask;
import io.druid.indexing.common.task.KillTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.loading.OmniSegmentLoader;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TaskLifecycleTest
{
  private static final Ordering<DataSegment> byIntervalOrdering = new Ordering<DataSegment>()
  {
    @Override
    public int compare(DataSegment dataSegment, DataSegment dataSegment2)
    {
      return Comparators.intervalsByStartThenEnd().compare(dataSegment.getInterval(), dataSegment2.getInterval());
    }
  };
  TaskStorageQueryAdapter tsqa = null;
  private File tmp = null;
  private TaskStorage ts = null;
  private TaskLockbox tl = null;
  private TaskQueue tq = null;
  private TaskRunner tr = null;
  private MockIndexerMetadataStorageCoordinator mdc = null;
  private TaskActionClientFactory tac = null;
  private TaskToolboxFactory tb = null;

  private static MockIndexerMetadataStorageCoordinator newMockMDC()
  {
    return new MockIndexerMetadataStorageCoordinator();
  }

  private static ServiceEmitter newMockEmitter()
  {
    return new ServiceEmitter(null, null, null)
    {
      @Override
      public void emit(Event event)
      {

      }

      @Override
      public void emit(ServiceEventBuilder builder)
      {

      }
    };
  }

  private static InputRow IR(String dt, String dim1, String dim2, float met)
  {
    return new MapBasedInputRow(
        new DateTime(dt).getMillis(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableMap.<String, Object>of(
            "dim1", dim1,
            "dim2", dim2,
            "met", met
        )
    );
  }

  private static FirehoseFactory newMockExceptionalFirehoseFactory()
  {
    return new FirehoseFactory()
    {
      @Override
      public Firehose connect(InputRowParser parser) throws IOException
      {
        return new Firehose()
        {
          @Override
          public boolean hasMore()
          {
            return true;
          }

          @Override
          public InputRow nextRow()
          {
            throw new RuntimeException("HA HA HA");
          }

          @Override
          public Runnable commit()
          {
            return new Runnable()
            {
              @Override
              public void run()
              {

              }
            };
          }

          @Override
          public void close() throws IOException
          {

          }
        };
      }
    };
  }

  private static FirehoseFactory newMockFirehoseFactory(final Iterable<InputRow> inputRows)
  {
    return new FirehoseFactory()
    {
      @Override
      public Firehose connect(InputRowParser parser) throws IOException
      {
        final Iterator<InputRow> inputRowIterator = inputRows.iterator();

        return new Firehose()
        {
          @Override
          public boolean hasMore()
          {
            return inputRowIterator.hasNext();
          }

          @Override
          public InputRow nextRow()
          {
            return inputRowIterator.next();
          }

          @Override
          public Runnable commit()
          {
            return new Runnable()
            {
              @Override
              public void run()
              {

              }
            };
          }

          @Override
          public void close() throws IOException
          {

          }
        };
      }
    };
  }

  @Before
  public void setUp() throws Exception
  {
    final ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);

    tmp = Files.createTempDir();

    final TaskQueueConfig tqc = new DefaultObjectMapper().readValue(
        "{\"startDelay\":\"PT0S\", \"restartDelay\":\"PT1S\"}",
        TaskQueueConfig.class
    );
    ts = new HeapMemoryTaskStorage(
        new TaskStorageConfig(null)
        {
        }
    );
    tsqa = new TaskStorageQueryAdapter(ts);
    tl = new TaskLockbox(ts);
    mdc = newMockMDC();
    tac = new LocalTaskActionClientFactory(ts, new TaskActionToolbox(tl, mdc, newMockEmitter()));
    tb = new TaskToolboxFactory(
        new TaskConfig(tmp.toString(), null, null, 50000, null),
        tac,
        newMockEmitter(),
        new DataSegmentPusher()
        {
          @Override
          public String getPathForHadoop(String dataSource)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public DataSegment push(File file, DataSegment segment) throws IOException
          {
            return segment;
          }
        },
        new DataSegmentKiller()
        {
          @Override
          public void kill(DataSegment segments) throws SegmentLoadingException
          {

          }
        },
        new DataSegmentMover()
        {
          @Override
          public DataSegment move(DataSegment dataSegment, Map<String, Object> targetLoadSpec)
              throws SegmentLoadingException
          {
            return dataSegment;
          }
        },
        new DataSegmentArchiver()
        {
          @Override
          public DataSegment archive(DataSegment segment) throws SegmentLoadingException
          {
            return segment;
          }

          @Override
          public DataSegment restore(DataSegment segment) throws SegmentLoadingException
          {
            return segment;
          }
        },
        null, // segment announcer
        null, // new segment server view
        null, // query runner factory conglomerate corporation unionized collective
        null, // query executor service
        null, // monitor scheduler
        new SegmentLoaderFactory(
            new OmniSegmentLoader(
                ImmutableMap.<String, DataSegmentPuller>of(
                    "local",
                    new LocalDataSegmentPuller()
                ),
                null,
                new SegmentLoaderConfig()
                {
                  @Override
                  public List<StorageLocationConfig> getLocations()
                  {
                    return Lists.newArrayList();
                  }
                }
            )
        ),
        new DefaultObjectMapper()
    );
    tr = new ThreadPoolTaskRunner(tb, null);
    tq = new TaskQueue(tqc, ts, tr, tac, tl, emitter);
    tq.start();
  }

  @After
  public void tearDown()
  {
    try {
      FileUtils.deleteDirectory(tmp);
    }
    catch (Exception e) {
      // suppress
    }
    tq.stop();
  }

  @Test
  public void testIndexTask() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        new IndexTask.IndexIngestionSpec(new DataSchema("foo", null,new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},new UniformGranularitySpec(
            Granularity.DAY,
            null,
            ImmutableList.of(new Interval("2010-01-01/P2D"))
        ) ),
                                         new IndexTask.IndexIOConfig(newMockFirehoseFactory(
                                             ImmutableList.of(
                                                 IR("2010-01-01T01", "x", "y", 1),
                                                 IR("2010-01-01T01", "x", "z", 1),
                                                 IR("2010-01-02T01", "a", "b", 2),
                                                 IR("2010-01-02T01", "a", "c", 1)
                                             )
                                         )),
                                         new IndexTask.IndexTuningConfig(10000, -1, -1)),
        TestUtils.MAPPER
    );

    final Optional<TaskStatus> preRunTaskStatus = tsqa.getStatus(indexTask.getId());
    Assert.assertTrue("pre run task status not present", !preRunTaskStatus.isPresent());

    final TaskStatus mergedStatus = runTask(indexTask);
    final TaskStatus status = ts.getStatus(indexTask.getId()).get();
    final List<DataSegment> publishedSegments = byIntervalOrdering.sortedCopy(mdc.getPublished());
    final List<DataSegment> loggedSegments = byIntervalOrdering.sortedCopy(tsqa.getInsertedSegments(indexTask.getId()));

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("merged statusCode", TaskStatus.Status.SUCCESS, mergedStatus.getStatusCode());
    Assert.assertEquals("segments logged vs published", loggedSegments, publishedSegments);
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());

    Assert.assertEquals("segment1 datasource", "foo", publishedSegments.get(0).getDataSource());
    Assert.assertEquals("segment1 interval", new Interval("2010-01-01/P1D"), publishedSegments.get(0).getInterval());
    Assert.assertEquals(
        "segment1 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(0).getDimensions()
    );
    Assert.assertEquals("segment1 metrics", ImmutableList.of("met"), publishedSegments.get(0).getMetrics());

    Assert.assertEquals("segment2 datasource", "foo", publishedSegments.get(1).getDataSource());
    Assert.assertEquals("segment2 interval", new Interval("2010-01-02/P1D"), publishedSegments.get(1).getInterval());
    Assert.assertEquals(
        "segment2 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(1).getDimensions()
    );
    Assert.assertEquals("segment2 metrics", ImmutableList.of("met"), publishedSegments.get(1).getMetrics());
  }

  @Test
  public void testIndexTaskFailure() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "foo",
                null,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularity.DAY,
                    null,
                    ImmutableList.of(new Interval("2010-01-01/P1D"))
                )
            ),
            new IndexTask.IndexIOConfig(newMockExceptionalFirehoseFactory()),
            new IndexTask.IndexTuningConfig(10000, -1, -1)
        ),
        TestUtils.MAPPER
    );

    final TaskStatus status = runTask(indexTask);

    Assert.assertEquals("statusCode", TaskStatus.Status.FAILED, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testKillTask() throws Exception
  {
    // This test doesn't actually do anything right now.  We should actually put things into the Mocked coordinator
    // Such that this test can test things...
    final Task killTask = new KillTask(null, "foo", new Interval("2010-01-02/P2D"));

    final TaskStatus status = runTask(killTask);
    Assert.assertEquals("merged statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testRealtimeishTask() throws Exception
  {
    final Task rtishTask = new RealtimeishTask();
    final TaskStatus status = runTask(rtishTask);

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testNoopTask() throws Exception
  {
    final Task noopTask = new DefaultObjectMapper().readValue(
        "{\"type\":\"noop\", \"runTime\":\"100\"}\"",
        Task.class
    );
    final TaskStatus status = runTask(noopTask);

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testNeverReadyTask() throws Exception
  {
    final Task neverReadyTask = new DefaultObjectMapper().readValue(
        "{\"type\":\"noop\", \"isReadyResult\":\"exception\"}\"",
        Task.class
    );
    final TaskStatus status = runTask(neverReadyTask);

    Assert.assertEquals("statusCode", TaskStatus.Status.FAILED, status.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testSimple() throws Exception
  {
    final Task task = new AbstractFixedIntervalTask(
        "id1",
        "id1",
        new TaskResource("id1", 1),
        "ds",
        new Interval("2012-01-01/P1D")
    )
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(
            toolbox.getTaskActionClient()
                   .submit(new LockListAction())
        );

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(new Interval("2012-01-01/P1D"))
                                               .version(myLock.getVersion())
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("segments published", 1, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testBadInterval() throws Exception
  {
    final Task task = new AbstractFixedIntervalTask("id1", "id1", "ds", new Interval("2012-01-01/P1D"))
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(new Interval("2012-01-01/P2D"))
                                               .version(myLock.getVersion())
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskStatus.Status.FAILED, status.getStatusCode());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testBadVersion() throws Exception
  {
    final Task task = new AbstractFixedIntervalTask("id1", "id1", "ds", new Interval("2012-01-01/P1D"))
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(new Interval("2012-01-01/P1D"))
                                               .version(myLock.getVersion() + "1!!!1!!")
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskStatus.Status.FAILED, status.getStatusCode());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  private TaskStatus runTask(final Task task) throws Exception
  {
    final Task dummyTask = new DefaultObjectMapper().readValue(
        "{\"type\":\"noop\", \"isReadyResult\":\"exception\"}\"",
        Task.class
    );
    final long startTime = System.currentTimeMillis();

    Preconditions.checkArgument(!task.getId().equals(dummyTask.getId()));

    tq.add(dummyTask);
    tq.add(task);

    TaskStatus retVal = null;

    for (final String taskId : ImmutableList.of(dummyTask.getId(), task.getId())) {
      try {
        TaskStatus status;
        while ((status = tsqa.getStatus(taskId).get()).isRunnable()) {
          if (System.currentTimeMillis() > startTime + 10 * 1000) {
            throw new ISE("Where did the task go?!: %s", task.getId());
          }

          Thread.sleep(100);
        }
        if (taskId.equals(task.getId())) {
          retVal = status;
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    return retVal;
  }

  private static class MockIndexerMetadataStorageCoordinator extends IndexerSQLMetadataStorageCoordinator
  {
    final private Set<DataSegment> published = Sets.newHashSet();
    final private Set<DataSegment> nuked = Sets.newHashSet();

    private MockIndexerMetadataStorageCoordinator()
    {
      super(null, null, null);
    }

    @Override
    public List<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval) throws IOException
    {
      return ImmutableList.of();
    }

    @Override
    public List<DataSegment> getUnusedSegmentsForInterval(String dataSource, Interval interval)
    {
      return ImmutableList.of();
    }

    @Override
    public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments)
    {
      Set<DataSegment> added = Sets.newHashSet();
      for (final DataSegment segment : segments) {
        if (published.add(segment)) {
          added.add(segment);
        }
      }

      return ImmutableSet.copyOf(added);
    }

    @Override
    public void deleteSegments(Set<DataSegment> segments)
    {
      nuked.addAll(segments);
    }

    public Set<DataSegment> getPublished()
    {
      return ImmutableSet.copyOf(published);
    }

    public Set<DataSegment> getNuked()
    {
      return ImmutableSet.copyOf(nuked);
    }
  }
}
