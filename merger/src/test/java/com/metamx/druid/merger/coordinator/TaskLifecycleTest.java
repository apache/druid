package com.metamx.druid.merger.coordinator;

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
import com.metamx.druid.input.InputRow;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.SegmentKiller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.actions.LocalTaskActionClient;
import com.metamx.druid.merger.common.actions.LockAcquireAction;
import com.metamx.druid.merger.common.actions.LockListAction;
import com.metamx.druid.merger.common.actions.LockReleaseAction;
import com.metamx.druid.merger.common.actions.SegmentInsertAction;
import com.metamx.druid.merger.common.actions.TaskActionToolbox;
import com.metamx.druid.merger.common.config.TaskConfig;
import com.metamx.druid.merger.common.task.AbstractTask;
import com.metamx.druid.merger.common.task.IndexTask;
import com.metamx.druid.merger.common.task.KillTask;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.exec.TaskConsumer;
import com.metamx.druid.realtime.Firehose;
import com.metamx.druid.realtime.FirehoseFactory;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.jets3t.service.ServiceException;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  private TaskToolbox tb = null;
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

    ts = new LocalTaskStorage();
    tl = new TaskLockbox(ts);
    tq = new TaskQueue(ts, tl);
    mdc = newMockMDC();

    tb = new TaskToolbox(
        new TaskConfig()
        {
          @Override
          public File getBaseTaskDir()
          {
            return tmp;
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
        new LocalTaskActionClient(ts, new TaskActionToolbox(tq, tl, mdc, newMockEmitter())),
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
        new SegmentKiller()
        {
          @Override
          public void kill(Collection<DataSegment> segments) throws ServiceException
          {

          }
        },
        new DefaultObjectMapper()
    )
    {
      @Override
      public Map<DataSegment, File> getSegments(
          Task task, List<DataSegment> segments
      ) throws SegmentLoadingException
      {
        return ImmutableMap.of();
      }
    };

    tr = new LocalTaskRunner(
        tb,
        Executors.newSingleThreadExecutor()
    );

    tc = new TaskConsumer(tq, tr, tb, newMockEmitter());
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
        "foo",
        new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P2D"))),
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
        "foo",
        new UniformGranularitySpec(Granularity.DAY, ImmutableList.of(new Interval("2010-01-01/P1D"))),
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
    // TODO: Worst test ever
    final Task killTask = new KillTask("foo", new Interval("2010-01-02/P2D"));

    final TaskStatus mergedStatus = runTask(killTask);
    Assert.assertEquals("merged statusCode", TaskStatus.Status.SUCCESS, mergedStatus.getStatusCode());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testRealtimeishTask() throws Exception
  {
    class RealtimeishTask extends AbstractTask
    {
      RealtimeishTask()
      {
        super("rt1", "rt", "foo", null);
      }

      @Override
      public String getType()
      {
        return "realtime_test";
      }

      @Override
      public TaskStatus run(TaskToolbox toolbox) throws Exception
      {
        final Interval interval1 = new Interval("2010-01-01T00/PT1H");
        final Interval interval2 = new Interval("2010-01-01T01/PT1H");

        // Sort of similar to what realtime tasks do:

        // Acquire lock for first interval
        final Optional<TaskLock> lock1 = toolbox.getTaskActionClient().submit(new LockAcquireAction(this, interval1));
        final List<TaskLock> locks1 = toolbox.getTaskActionClient().submit(new LockListAction(this));

        // (Confirm lock sanity)
        Assert.assertTrue("lock1 present", lock1.isPresent());
        Assert.assertEquals("lock1 interval", interval1, lock1.get().getInterval());
        Assert.assertEquals("locks1", ImmutableList.of(lock1.get()), locks1);

        // Acquire lock for second interval
        final Optional<TaskLock> lock2 = toolbox.getTaskActionClient().submit(new LockAcquireAction(this, interval2));
        final List<TaskLock> locks2 = toolbox.getTaskActionClient().submit(new LockListAction(this));

        // (Confirm lock sanity)
        Assert.assertTrue("lock2 present", lock2.isPresent());
        Assert.assertEquals("lock2 interval", interval2, lock2.get().getInterval());
        Assert.assertEquals("locks2", ImmutableList.of(lock1.get(), lock2.get()), locks2);

        // Push first segment
        toolbox.getTaskActionClient()
               .submit(
                   new SegmentInsertAction(
                       this,
                       ImmutableSet.of(
                           DataSegment.builder()
                                      .dataSource("foo")
                                      .interval(interval1)
                                      .version(lock1.get().getVersion())
                                      .build()
                       )
                   )
               );

        // Release first lock
        toolbox.getTaskActionClient().submit(new LockReleaseAction(this, interval1));
        final List<TaskLock> locks3 = toolbox.getTaskActionClient().submit(new LockListAction(this));

        // (Confirm lock sanity)
        Assert.assertEquals("locks3", ImmutableList.of(lock2.get()), locks3);

        // Push second segment
        toolbox.getTaskActionClient()
               .submit(
                   new SegmentInsertAction(
                       this,
                       ImmutableSet.of(
                           DataSegment.builder()
                                      .dataSource("foo")
                                      .interval(interval2)
                                      .version(lock2.get().getVersion())
                                      .build()
                       )
                   )
               );

        // Release second lock
        toolbox.getTaskActionClient().submit(new LockReleaseAction(this, interval2));
        final List<TaskLock> locks4 = toolbox.getTaskActionClient().submit(new LockListAction(this));

        // (Confirm lock sanity)
        Assert.assertEquals("locks4", ImmutableList.<TaskLock>of(), locks4);

        // Exit
        return TaskStatus.success(getId());
      }
    }

    final Task rtishTask = new RealtimeishTask();
    final TaskStatus status = runTask(rtishTask);

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testSimple() throws Exception
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
        final TaskLock myLock = Iterables.getOnlyElement(
            toolbox.getTaskActionClient()
                   .submit(new LockListAction(this))
        );

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(new Interval("2012-01-01/P1D"))
                                               .version(myLock.getVersion())
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(this, ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("segments published", 1, mdc.getPublished().size());
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
        final TaskLock myLock = Iterables.getOnlyElement(
            toolbox.getTaskActionClient()
                   .submit(new LockListAction(this))
        );

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(new Interval("2012-01-01/P2D"))
                                               .version(myLock.getVersion())
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(this, ImmutableSet.of(segment)));
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskStatus.Status.FAILED, status.getStatusCode());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
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
        final TaskLock myLock = Iterables.getOnlyElement(
            toolbox.getTaskActionClient()
                   .submit(new LockListAction(this))
        );

        final DataSegment segment = DataSegment.builder()
                                               .dataSource("ds")
                                               .interval(new Interval("2012-01-01/P1D"))
                                               .version(myLock.getVersion() + "1!!!1!!")
                                               .build();

        toolbox.getTaskActionClient().submit(new SegmentInsertAction(this, ImmutableSet.of(segment)));
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
    public void announceHistoricalSegments(Set<DataSegment> segment)
    {
      published.addAll(segment);
    }

    @Override
    public void deleteSegments(Set<DataSegment> segment)
    {
      nuked.addAll(segment);
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
