package com.metamx.druid.merger.worker;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.worker.config.WorkerConfig;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class Worker
{
  private static final Logger log = new Logger(Worker.class);

  private final String host;

  private final ConcurrentHashMap<String, TaskStatus> runningTasks;

  public Worker(
      WorkerConfig config
  )
  {
    this(
        config.getHost()
    );
  }

  @JsonCreator
  public Worker(
      @JsonProperty("host") String host
  )
  {
    this.host = host;
    this.runningTasks = new ConcurrentHashMap<String, TaskStatus>();
  }

  @JsonProperty
  public String getHost()
  {
    return host;
  }

  public Map<String, TaskStatus> getTasks()
  {
    return runningTasks;
  }

  public Map<String, String> getStringProps()
  {
    return ImmutableMap.<String, String>of(
        "host", host
    );
  }

  public TaskStatus addTask(TaskStatus status)
  {
    return runningTasks.put(status.getId(), status);
  }

  public TaskStatus removeTask(String taskId)
  {
    return runningTasks.remove(taskId);
  }
}
