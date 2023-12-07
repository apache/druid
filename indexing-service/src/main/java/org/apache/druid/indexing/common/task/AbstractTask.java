/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.UpdateLocationAction;
import org.apache.druid.indexing.common.actions.UpdateStatusAction;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.segment.indexing.BatchIOConfig;
import org.apache.druid.server.DruidNode;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class AbstractTask implements Task
{

  private static final Logger log = new Logger(AbstractTask.class);

  // This is mainly to avoid using combinations of IOConfig flags to figure out the ingestion mode and
  // also to use the mode as dimension in metrics
  public enum IngestionMode
  {
    REPLACE, // replace with tombstones
    APPEND, // append to existing segments
    REPLACE_LEGACY, // original replace, it does not replace existing data for empty time chunks in input intervals
    HADOOP, // non-native batch, hadoop ingestion
    NONE; // not an ingestion task (i.e. a kill task)

    @JsonCreator
    public static IngestionMode fromString(String name)
    {
      if (name == null) {
        return null;
      }
      return valueOf(StringUtils.toUpperCase(name));
    }
  }
  private final IngestionMode ingestionMode;

  @JsonIgnore
  private final String id;

  @JsonIgnore
  private final String groupId;

  @JsonIgnore
  private final TaskResource taskResource;

  @JsonIgnore
  private final String dataSource;

  private final Map<String, Object> context;

  private File reportsFile;
  private File statusFile;

  private final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();

  private volatile CountDownLatch cleanupCompletionLatch;

  protected AbstractTask(String id, String dataSource, Map<String, Object> context, IngestionMode ingestionMode)
  {
    this(id, null, null, dataSource, context, ingestionMode);
  }

  protected AbstractTask(String id, String dataSource, Map<String, Object> context)
  {
    this(id, null, null, dataSource, context, IngestionMode.NONE);
  }

  protected AbstractTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context,
      @Nonnull IngestionMode ingestionMode
  )
  {
    this.id = IdUtils.validateId("task ID", id);
    this.groupId = groupId == null ? id : groupId;
    this.taskResource = taskResource == null ? new TaskResource(id, 1) : taskResource;
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    // Copy the given context into a new mutable map because the Druid indexing service can add some internal contexts.
    this.context = context == null ? new HashMap<>() : new HashMap<>(context);
    this.ingestionMode = ingestionMode;
    IndexTaskUtils.setTaskDimensions(metricBuilder, this);
  }

  protected AbstractTask(
      String id,
      @Nullable String groupId,
      @Nullable TaskResource taskResource,
      String dataSource,
      @Nullable Map<String, Object> context
  )
  {
    this(id, groupId, taskResource, dataSource, context, IngestionMode.NONE);
  }

  @Nullable
  public String setup(TaskToolbox toolbox) throws Exception
  {
    if (toolbox.getConfig().isEncapsulatedTask()) {
      File taskDir = toolbox.getConfig().getTaskDir(getId());
      FileUtils.mkdirp(taskDir);
      File attemptDir = Paths.get(taskDir.getAbsolutePath(), "attempt", toolbox.getAttemptId()).toFile();
      FileUtils.mkdirp(attemptDir);
      reportsFile = new File(attemptDir, "report.json");
      statusFile = new File(attemptDir, "status.json");
      InetAddress hostName = InetAddress.getLocalHost();
      DruidNode node = toolbox.getTaskExecutorNode();
      toolbox.getTaskActionClient().submit(new UpdateLocationAction(TaskLocation.create(
          hostName.getHostAddress(), node.getPlaintextPort(), node.getTlsPort(), node.isEnablePlaintextPort()
      )));
    }
    log.debug("Task setup complete");
    return null;
  }

  @Override
  public final TaskStatus run(TaskToolbox taskToolbox) throws Exception
  {
    TaskStatus taskStatus = null;
    try {
      cleanupCompletionLatch = new CountDownLatch(1);
      String errorMessage = setup(taskToolbox);
      if (org.apache.commons.lang3.StringUtils.isNotBlank(errorMessage)) {
        taskStatus = TaskStatus.failure(getId(), errorMessage);
        return taskStatus;
      }
      taskStatus = runTask(taskToolbox);
      return taskStatus;
    }
    catch (Exception e) {
      taskStatus = TaskStatus.failure(getId(), e.toString());
      throw e;
    }
    finally {
      try {
        cleanUp(taskToolbox, taskStatus);
      }
      finally {
        cleanupCompletionLatch.countDown();
      }
    }
  }

  public abstract TaskStatus runTask(TaskToolbox taskToolbox) throws Exception;

  @Override
  public void cleanUp(TaskToolbox toolbox, @Nullable TaskStatus taskStatus) throws Exception
  {
    // clear any interrupted status to ensure subsequent cleanup proceeds without interruption.
    Thread.interrupted();

    if (!toolbox.getConfig().isEncapsulatedTask()) {
      log.debug("Not pushing task logs and reports from task.");
      return;
    }

    TaskStatus taskStatusToReport = taskStatus == null
        ? TaskStatus.failure(id, "Task failed to run")
        : taskStatus;
    // report back to the overlord
    UpdateStatusAction status = new UpdateStatusAction("", taskStatusToReport);
    toolbox.getTaskActionClient().submit(status);
    toolbox.getTaskActionClient().submit(new UpdateLocationAction(TaskLocation.unknown()));

    if (reportsFile != null && reportsFile.exists()) {
      toolbox.getTaskLogPusher().pushTaskReports(id, reportsFile);
      log.debug("Pushed task reports");
    } else {
      log.debug("No task reports file exists to push");
    }

    if (statusFile != null) {
      toolbox.getJsonMapper().writeValue(statusFile, taskStatusToReport);
      toolbox.getTaskLogPusher().pushTaskStatus(id, statusFile);
      Files.deleteIfExists(statusFile.toPath());
      log.debug("Pushed task status");
    } else {
      log.debug("No task status file exists to push");
    }
  }

  @Override
  public boolean waitForCleanupToFinish()
  {
    try {
      if (cleanupCompletionLatch != null) {
        // block until the cleanup process completes
        return cleanupCompletionLatch.await(300, TimeUnit.SECONDS);
      }

      return true;
    }
    catch (InterruptedException e) {
      log.warn("Interrupted while waiting for task cleanUp to finish!");
      Thread.currentThread().interrupt();
      return false;
    }
  }

  public static String getOrMakeId(@Nullable String id, final String typeName, String dataSource)
  {
    return getOrMakeId(id, typeName, dataSource, null);
  }

  static String getOrMakeId(@Nullable String id, final String typeName, String dataSource, @Nullable Interval interval)
  {
    if (id != null) {
      return id;
    }

    return IdUtils.newTaskId(typeName, dataSource, interval);
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getGroupId()
  {
    return groupId;
  }

  @JsonProperty("resource")
  @Override
  public TaskResource getTaskResource()
  {
    return taskResource;
  }

  @Override
  public String getNodeType()
  {
    return null;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(Query<T> query)
  {
    return null;
  }

  @Override
  public boolean supportsQueries()
  {
    return false;
  }

  @Override
  public String getClasspathPrefix()
  {
    return null;
  }

  @Override
  public boolean canRestore()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "AbstractTask{" +
        "id='" + id + '\'' +
        ", groupId='" + groupId + '\'' +
        ", taskResource=" + taskResource +
        ", dataSource='" + dataSource + '\'' +
        ", context=" + context +
        '}';
  }

  public TaskStatus success()
  {
    return TaskStatus.success(getId());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractTask that = (AbstractTask) o;

    if (!id.equals(that.id)) {
      return false;
    }

    if (!groupId.equals(that.groupId)) {
      return false;
    }

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    return context.equals(that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(id, groupId, dataSource, context);
  }

  public static List<TaskLock> getTaskLocks(TaskActionClient client) throws IOException
  {
    return client.submit(new LockListAction());
  }

  @Override
  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
  }

  /**
   * Whether maximum memory usage should be considered in estimation for indexing tasks.
   */
  protected boolean isUseMaxMemoryEstimates()
  {
    return getContextValue(
        Tasks.USE_MAX_MEMORY_ESTIMATES,
        Tasks.DEFAULT_USE_MAX_MEMORY_ESTIMATES
    );
  }

  protected ServiceMetricEvent.Builder getMetricBuilder()
  {
    return metricBuilder;
  }

  public IngestionMode getIngestionMode()
  {
    return ingestionMode;
  }

  protected static IngestionMode computeCompactionIngestionMode(@Nullable CompactionIOConfig ioConfig)
  {
    // CompactionIOConfig does not have an isAppendToExisting method, so use default (for batch since compaction
    // is basically batch ingestion)
    final boolean isAppendToExisting = BatchIOConfig.DEFAULT_APPEND_EXISTING;
    final boolean isDropExisting = ioConfig == null ? BatchIOConfig.DEFAULT_DROP_EXISTING : ioConfig.isDropExisting();
    return computeIngestionMode(isAppendToExisting, isDropExisting);
  }

  protected static IngestionMode computeBatchIngestionMode(@Nullable BatchIOConfig ioConfig)
  {
    final boolean isAppendToExisting = ioConfig == null
        ? BatchIOConfig.DEFAULT_APPEND_EXISTING
        : ioConfig.isAppendToExisting();
    final boolean isDropExisting = ioConfig == null ? BatchIOConfig.DEFAULT_DROP_EXISTING : ioConfig.isDropExisting();
    return computeIngestionMode(isAppendToExisting, isDropExisting);
  }

  private static IngestionMode computeIngestionMode(boolean isAppendToExisting, boolean isDropExisting)
  {
    if (!isAppendToExisting && isDropExisting) {
      return IngestionMode.REPLACE;
    } else if (isAppendToExisting && !isDropExisting) {
      return IngestionMode.APPEND;
    } else if (!isAppendToExisting) {
      return IngestionMode.REPLACE_LEGACY;
    }
    throw new IAE("Cannot simultaneously replace and append to existing segments. "
        + "Either dropExisting or appendToExisting should be set to false");
  }

  public void emitMetric(
      ServiceEmitter emitter,
      String metric,
      Number value
  )
  {

    if (emitter == null || metric == null || value == null) {
      return;
    }
    emitter.emit(getMetricBuilder().setMetric(metric, value));
  }


}
