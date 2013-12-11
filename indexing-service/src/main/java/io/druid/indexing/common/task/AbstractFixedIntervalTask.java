package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexing.common.actions.LockTryAcquireAction;
import io.druid.indexing.common.actions.TaskActionClient;
import org.joda.time.Interval;

public abstract class AbstractFixedIntervalTask extends AbstractTask
{
  @JsonIgnore
  final Interval interval;

  protected AbstractFixedIntervalTask(
      String id,
      String dataSource,
      Interval interval
  )
  {
    this(id, id, new TaskResource(id, 1), dataSource, interval);
  }

  protected AbstractFixedIntervalTask(
      String id,
      String groupId,
      String dataSource,
      Interval interval
  )
  {
    this(id, groupId, new TaskResource(id, 1), dataSource, interval);
  }

  protected AbstractFixedIntervalTask(
      String id,
      String groupId,
      TaskResource taskResource,
      String dataSource,
      Interval interval
  )
  {
    super(id, groupId, taskResource, dataSource);
    this.interval = interval;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return taskActionClient.submit(new LockTryAcquireAction(interval)).isPresent();
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }
}
