package com.metamx.druid.merger.coordinator;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.task.AbstractTask;
import com.metamx.druid.merger.coordinator.exec.TaskConsumer;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

public class TaskConsumerTest
{
  private TaskStorage ts = null;
  private TaskQueue tq = null;
  private TaskRunner tr = null;
  private MockMergerDBCoordinator mdc = null;
  private TaskConsumer tc = null;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(EasyMock.createMock(ServiceEmitter.class));

    ts = new LocalTaskStorage();
    tq = new TaskQueue(ts);
    tr = new LocalTaskRunner(
        new TaskToolbox(null, null, null, null, null, null),
        Executors.newSingleThreadExecutor()
    );

    mdc = newMockMDC();
    tc = new TaskConsumer(tq, tr, mdc, newMockEmitter());

    tq.start();
    tc.start();
  }

  @After
  public void tearDown()
  {
    tc.stop();
    tq.stop();
  }

  @Test
  public void testSimple() throws Exception
  {
    tq.add(
        new AbstractTask("id1", "id1", "ds", new Interval("2012-01-01/P1D"))
        {
          @Override
          public Type getType()
          {
            return Type.TEST;
          }

          @Override
          public TaskStatus run(
              TaskContext context, TaskToolbox toolbox, TaskCallback callback
          ) throws Exception
          {
            return TaskStatus.success(getId()).withSegments(
                ImmutableSet.of(
                    DataSegment.builder()
                               .dataSource("ds")
                               .interval(new Interval("2012-01-01/P1D"))
                               .version(context.getVersion())
                               .build()
                )
            );
          }
        }
    );

    while (ts.getStatus("id1").get().isRunnable()) {
      Thread.sleep(100);
    }

    final TaskStatus status = ts.getStatus("id1").get();
    Assert.assertEquals("statusCode", TaskStatus.Status.SUCCESS, status.getStatusCode());
    Assert.assertEquals("nextTasks.size", 0, status.getNextTasks().size());
    Assert.assertEquals("segments.size", 1, status.getSegments().size());
    Assert.assertEquals("segmentsNuked.size", 0, status.getSegmentsNuked().size());
    Assert.assertEquals("segments published", status.getSegments(), mdc.getPublished());
    Assert.assertEquals("segments nuked", status.getSegmentsNuked(), mdc.getNuked());
  }

  @Test
  public void testBadVersion() throws Exception
  {
    tq.add(
        new AbstractTask("id1", "id1", "ds", new Interval("2012-01-01/P1D"))
        {
          @Override
          public Type getType()
          {
            return Type.TEST;
          }

          @Override
          public TaskStatus run(
              TaskContext context, TaskToolbox toolbox, TaskCallback callback
          ) throws Exception
          {
            return TaskStatus.success(getId()).withSegments(
                ImmutableSet.of(
                    DataSegment.builder()
                               .dataSource("ds")
                               .interval(new Interval("2012-01-01/P1D"))
                               .version(context.getVersion() + "1!!!1!!")
                               .build()
                )
            );
          }
        }
    );

    while (ts.getStatus("id1").get().isRunnable()) {
      Thread.sleep(100);
    }

    final TaskStatus status = ts.getStatus("id1").get();
    Assert.assertEquals("statusCode", TaskStatus.Status.FAILED, status.getStatusCode());
    Assert.assertEquals("nextTasks.size", 0, status.getNextTasks().size());
    Assert.assertEquals("segments.size", 0, status.getSegments().size());
    Assert.assertEquals("segmentsNuked.size", 0, status.getSegmentsNuked().size());
    Assert.assertEquals("segments published", status.getSegments(), mdc.getPublished());
    Assert.assertEquals("segments nuked", status.getSegmentsNuked(), mdc.getNuked());
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
    public void commitTaskStatus(TaskStatus taskStatus)
    {
      for(final DataSegment segment : taskStatus.getSegments())
      {
        announceHistoricalSegment(segment);
      }

      for(final DataSegment segment : taskStatus.getSegmentsNuked())
      {
        deleteSegment(segment);
      }
    }

    @Override
    public void announceHistoricalSegment(DataSegment segment)
    {
      published.add(segment);
    }

    @Override
    public void deleteSegment(DataSegment segment)
    {
      nuked.add(segment);
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

  private MockMergerDBCoordinator newMockMDC()
  {
    return new MockMergerDBCoordinator();
  }

  private ServiceEmitter newMockEmitter()
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
}
