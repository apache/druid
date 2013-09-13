/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.coordinator;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.DoubleSumAggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.indexing.common.task.TaskResource;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.DataSegmentKiller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.indexing.common.TaskLock;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.TaskToolbox;
import com.metamx.druid.indexing.common.TaskToolboxFactory;
import com.metamx.druid.indexing.common.actions.LocalTaskActionClientFactory;
import com.metamx.druid.indexing.common.actions.LockAcquireAction;
import com.metamx.druid.indexing.common.actions.LockListAction;
import com.metamx.druid.indexing.common.actions.LockReleaseAction;
import com.metamx.druid.indexing.common.actions.SegmentInsertAction;
import com.metamx.druid.indexing.common.actions.TaskActionClientFactory;
import com.metamx.druid.indexing.common.actions.TaskActionToolbox;
import com.metamx.druid.indexing.common.config.TaskConfig;
import com.metamx.druid.indexing.common.task.AbstractTask;
import com.metamx.druid.indexing.common.task.IndexTask;
import com.metamx.druid.indexing.common.task.KillTask;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.exec.TaskConsumer;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
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
import java.util.Set;
import java.util.concurrent.Executors;

public class TaskLifecycleTest
{
  private File tmp = null;
  private TaskStorage ts = null;
  private TaskLockbox tl = null;
  private TaskQueue tq = null;
  private TaskRunner tr = null;
  private MockMergerDBCoordinator mdc = null;
  private TaskActionClientFactory tac = null;
  private TaskToolboxFactory tb = null;
  private TaskConsumer tc = null;
  TaskStorageQueryAdapter tsqa = null;

  private static final Ordering<DataSegment> byIntervalOrdering = new Ordering<DataSegment>()
  {
    @Override
    public int compare(DataSegment dataSegment, DataSegment dataSegment2)
    {
      return Comparators.intervalsByStartThenEnd().compare(dataSegment.getInterval(), dataSegment2.getInterval());
    }
  };

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(EasyMock.createMock(ServiceEmitter.class));

    tmp = Files.createTempDir();

    ts = new HeapMemoryTaskStorage();
    tl = new TaskLockbox(ts);
    tq = new TaskQueue(ts, tl);
    mdc = newMockMDC();
    tac = new LocalTaskActionClientFactory(ts, new TaskActionToolbox(tq, tl, mdc, newMockEmitter()));

    tb = new TaskToolboxFactory(
        new TaskConfig()
        {
          @Override
          public String getBaseDir()
          {
            return tmp.toString();
          }

          @Override
          public int getDefaultRowFlushBoundary()
          {
            return 50000;
          }

          @Override
          public String getHadoopWorkingPath()
          {
            return null;
          }
        },
        tac,
        newMockEmitter(),
        null, // s3 client
        new DataSegmentPusher()
        {
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
        null, // segment announcer
        null, // new segment server view
        null, // query runner factory conglomerate corporation unionized collective
        null, // query executor service
        null, // monitor scheduler
        new DefaultObjectMapper()
    );

    tr = new ThreadPoolTaskRunner(
        tb,
        Executors.newSingleThreadExecutor()
    );

    tc = new TaskConsumer(tq, tr, tac, newMockEmitter());
    tsqa = new TaskStorageQueryAdapter(ts);

    tq.start();
    tc.start();
  }

  @After
  public void tearDown()
  {
    try {
      FileUtils.deleteDirectory(tmp);
    } catch(Exception e) {
      // suppress
    }
    tc.stop();
    tq.stop();
  }

  @Test
  public void testIndexTask() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        "foo",
        new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P2D"))),
        null,
        new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
        QueryGranularity.NONE,
        10000,
        newMockFirehoseFactory(
            ImmutableList.of(
                IR("2010-01-01T01", "x", "y", 1),
                IR("2010-01-01T01", "x", "z", 1),
                IR("2010-01-02T01", "a", "b", 2),
                IR("2010-01-02T01", "a", "c", 1)
            )
        ),
        -1
    );

    final Optional<TaskStatus> preRunTaskStatus = tsqa.getSameGroupMergedStatus(indexTask.getId());
    Assert.assertTrue("pre run task status not present", !preRunTaskStatus.isPresent());

    final TaskStatus mergedStatus = runTask(indexTask);
    final TaskStatus status = ts.getStatus(indexTask.getId()).get();
    final List<DataSegment> publishedSegments = byIntervalOrdering.sortedCopy(mdc.getPublished());
    final List<DataSegment> loggedSegments = byIntervalOrdering.sortedCopy(tsqa.getSameGroupNewSegments(indexTask.getId()));

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("merged statusCode", TaskStatus.Status.SUCCESS, mergedStatus.getStatusCode());
    Assert.assertEquals("segments logged vs published", loggedSegments, publishedSegments);
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());

    Assert.assertEquals("segment1 datasource", "foo", publishedSegments.get(0).getDataSource());
    Assert.assertEquals("segment1 interval", new Interval("2010-01-01/P1D"), publishedSegments.get(0).getInterval());
    Assert.assertEquals("segment1 dimensions", ImmutableList.of("dim1", "dim2"), publishedSegments.get(0).getDimensions());
    Assert.assertEquals("segment1 metrics", ImmutableList.of("met"), publishedSegments.get(0).getMetrics());

    Assert.assertEquals("segment2 datasource", "foo", publishedSegments.get(1).getDataSource());
    Assert.assertEquals("segment2 interval", new Interval("2010-01-02/P1D"), publishedSegments.get(1).getInterval());
    Assert.assertEquals("segment2 dimensions", ImmutableList.of("dim1", "dim2"), publishedSegments.get(1).getDimensions());
    Assert.assertEquals("segment2 metrics", ImmutableList.of("met"), publishedSegments.get(1).getMetrics());
  }

  @Test
  public void testIndexTaskFailure() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        "foo",
        new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P1D"))),
        null,
        new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
        QueryGranularity.NONE,
        10000,
        newMockExceptionalFirehoseFactory(),
        -1
    );

    final TaskStatus mergedStatus = runTask(indexTask);
    final TaskStatus status = ts.getStatus(indexTask.getId()).get();

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("merged statusCode", TaskStatus.Status.FAILED, mergedStatus.getStatusCode());
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
  public void testSimple() throws Exception
  {
    final Task task = new AbstractTask("id1", "id1", new TaskResource("id1", 1), "ds", new Interval("2012-01-01/P1D"))
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
    final Task task = new AbstractTask("id1", "id1", "ds", new Interval("2012-01-01/P1D"))
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
    final Task task = new AbstractTask("id1", "id1", "ds", new Interval("2012-01-01/P1D"))
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

  private TaskStatus runTask(Task task)
  {
    final long startTime = System.currentTimeMillis();

    tq.add(task);

    TaskStatus status;

    try {
      while ( (status = tsqa.getSameGroupMergedStatus(task.getId()).get()).isRunnable()) {
        if(System.currentTimeMillis() > startTime + 10 * 1000) {
          throw new ISE("Where did the task go?!: %s", task.getId());
        }

        Thread.sleep(100);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return status;
  }

  private static class MockMergerDBCoordinator extends MergerDBCoordinator
  {
    final private Set<DataSegment> published = Sets.newHashSet();
    final private Set<DataSegment> nuked = Sets.newHashSet();

    private MockMergerDBCoordinator()
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
      for(final DataSegment segment : segments) {
        if(published.add(segment)) {
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

  private static MockMergerDBCoordinator newMockMDC()
  {
    return new MockMergerDBCoordinator();
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
      public Firehose connect() throws IOException
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
            return new Runnable() {
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
      public Firehose connect() throws IOException
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
}
