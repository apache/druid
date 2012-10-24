package com.metamx.druid.merger.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.druid.merger.common.task.Task;
import org.joda.time.Interval;

import java.util.Set;
import java.util.TreeSet;

/**
 * Represents a transaction as well as the lock it holds. Not immutable: the task set can change.
 */
public class TaskGroup
{
  private final String groupId;
  private final String dataSource;
  private final Interval interval;
  private final String version;
  private final Set<Task> taskSet = new TreeSet<Task>(
      new Ordering<Task>()
      {
        @Override
        public int compare(Task task, Task task1)
        {
          return task.getId().compareTo(task1.getId());
        }
      }.nullsFirst()
  );

  public TaskGroup(String groupId, String dataSource, Interval interval, String version)
  {
    this.groupId = groupId;
    this.dataSource = dataSource;
    this.interval = interval;
    this.version = version;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public Interval getInterval()
  {
    return interval;
  }

  public String getVersion()
  {
    return version;
  }

  public Set<Task> getTaskSet()
  {
    return taskSet;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("groupId", groupId)
                  .add("dataSource", dataSource)
                  .add("interval", interval)
                  .add("version", version)
                  .add(
                      "taskSet",
                      Lists.newArrayList(
                          Iterables.transform(
                              taskSet, new Function<Task, Object>()
                          {
                            @Override
                            public Object apply(Task task)
                            {
                              return task.getId();
                            }
                          }
                          )
                      )
                  )
                  .toString();
  }
}
