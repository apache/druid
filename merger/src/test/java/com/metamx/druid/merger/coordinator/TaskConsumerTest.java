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
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import junit.framework.Assert;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

public class TaskConsumerTest
{
  @Test
  public void testSimple()
  {
    final TaskStorage ts = new LocalTaskStorage();
    final TaskQueue tq = new TaskQueue(ts);
    final TaskRunner tr = new LocalTaskRunner(
        new TaskToolbox(null, null, null, null, null, null),
        Executors.newSingleThreadExecutor()
    );

    final MockMergerDBCoordinator mdc = newMockMDC();
    final TaskConsumer tc = new TaskConsumer(tq, tr, mdc, newMockEmitter());

    tq.start();
    tc.start();

    try {
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
      Assert.assertTrue("nextTasks", status.getNextTasks().isEmpty());
      Assert.assertEquals("segments.size", 1, status.getSegments().size());
      Assert.assertEquals("segmentsNuked.size", 0, status.getSegmentsNuked().size());
      Assert.assertEquals("segments published", status.getSegments(), mdc.getPublished());
      Assert.assertEquals("segments nuked", status.getSegmentsNuked(), mdc.getNuked());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      tc.stop();
      tq.stop();
    }
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
    public void announceHistoricalSegment(DataSegment segment) throws Exception
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
