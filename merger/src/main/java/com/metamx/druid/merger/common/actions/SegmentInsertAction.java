package com.metamx.druid.merger.common.actions;

import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.type.TypeReference;

import java.util.List;
import java.util.Set;

public class SegmentInsertAction implements TaskAction<Void>
{
  private final Task task;
  private final Set<DataSegment> segments;

  @JsonCreator
  public SegmentInsertAction(
      @JsonProperty("task") Task task,
      @JsonProperty("segments") Set<DataSegment> segments
  )
  {
    this.task = task;
    this.segments = ImmutableSet.copyOf(segments);
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>() {};
  }

  @Override
  public Void perform(TaskActionToolbox toolbox)
  {
    // Verify that each of these segments-to-insert falls under some lock
    // TODO: Should this be done while holding the giant lock on TaskLockbox? (To prevent anyone from grabbing
    // TODO: these locks out from under us while the operation is ongoing.) Probably not necessary.
    final List<TaskLock> taskLocks = toolbox.getTaskLockbox().findLocksForTask(task);
    for(final DataSegment segment : segments) {
      final boolean ok = Iterables.any(
          taskLocks, new Predicate<TaskLock>()
      {
        @Override
        public boolean apply(TaskLock taskLock)
        {
          return taskLock.getVersion().equals(segment.getVersion())
                 && taskLock.getDataSource().equals(segment.getDataSource())
                 && taskLock.getInterval().contains(segment.getInterval());
        }
      }
      );

      if(!ok) {
        throw new ISE("No currently-held lock covers segment: %s", segment);
      }
    }

    try {
      toolbox.getMergerDBCoordinator().announceHistoricalSegments(segments);

      // Emit metrics
      final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
          .setUser2(task.getDataSource())
          .setUser4(task.getType());

      for (DataSegment segment : segments) {
        metricBuilder.setUser5(segment.getInterval().toString());
        toolbox.getEmitter().emit(metricBuilder.build("indexer/segment/bytes", segment.getSize()));
      }

      return null;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
