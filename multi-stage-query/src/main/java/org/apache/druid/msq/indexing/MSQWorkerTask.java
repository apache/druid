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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.msq.exec.WorkerRunRef;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@JsonTypeName(MSQWorkerTask.TYPE)
public class MSQWorkerTask extends AbstractTask
{
  public static final String TYPE = "query_worker";

  private final String controllerTaskId;
  private final int workerNumber;
  private final int retryCount;
  private final WorkerRunRef workerRunRef = new WorkerRunRef();

  // Using an Injector directly because tasks do not have a way to provide their own Guice modules.
  // Not part of equals and hashcode implementation
  @JacksonInject
  private Injector injector;

  @JsonCreator
  @VisibleForTesting
  public MSQWorkerTask(
      @JsonProperty("controllerTaskId") final String controllerTaskId,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("workerNumber") final int workerNumber,
      @JsonProperty("context") final Map<String, Object> context,
      @JsonProperty(value = "retry", defaultValue = "0") final int retryCount
  )
  {
    super(
        MSQTasks.workerTaskId(controllerTaskId, workerNumber, retryCount),
        controllerTaskId,
        null,
        dataSource,
        context
    );

    this.controllerTaskId = controllerTaskId;
    this.workerNumber = workerNumber;
    this.retryCount = retryCount;
  }

  @JsonProperty
  public String getControllerTaskId()
  {
    return controllerTaskId;
  }

  @JsonProperty
  public int getWorkerNumber()
  {
    return workerNumber;
  }

  @JsonProperty("retry")
  public int getRetryCount()
  {
    return retryCount;
  }

  /**
   * Creates a new retry {@link MSQWorkerTask} with the same context as the current task, but with the retry count
   * incremented by 1
   */
  public MSQWorkerTask getRetryTask()
  {
    return new MSQWorkerTask(controllerTaskId, getDataSource(), workerNumber, getContext(), retryCount + 1);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    // the input sources are properly computed in the SQL / calcite layer, but not in the native MSQ task here.
    return ImmutableSet.of();
  }

  @Override
  public boolean isReady(final TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus runTask(final TaskToolbox toolbox)
  {
    emitMetric(toolbox.getEmitter(), "ingest/count", 1);
    try (final IndexerWorkerContext context = IndexerWorkerContext.createProductionInstance(this, toolbox, injector)) {
      // We'll run the worker in a separate thread, so we can interrupt the thread on shutdown or controller failure.
      final ListeningExecutorService workerExec =
          MoreExecutors.listeningDecorator(
              Execs.singleThreaded("query-worker[" + StringUtils.encodeForFormat(getId()) + "]-%s"));

      try {
        final Worker worker = new WorkerImpl(this, context);
        final ListenableFuture<?> future = workerRunRef.run(worker, workerExec);

        try (final PeriodicControllerChecker ignored = openControllerChecker(context)) {
          FutureUtils.getUnchecked(future, true);
        }

        return TaskStatus.success(getId());
      }
      finally {
        workerExec.shutdown();
      }
    }
    catch (MSQException e) {
      return TaskStatus.failure(getId(), MSQFaultUtils.generateMessageWithErrorCode(e.getFault()));
    }
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    workerRunRef.cancel();
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  @Override
  public boolean supportsQueries()
  {
    // Even though we don't have a QueryResource, we do embed a query stack, and so we need preloaded resources
    // such as broadcast tables.
    return true;
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
    if (!super.equals(o)) {
      return false;
    }
    MSQWorkerTask that = (MSQWorkerTask) o;
    return workerNumber == that.workerNumber
           && retryCount == that.retryCount
           && Objects.equals(controllerTaskId, that.controllerTaskId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), controllerTaskId, workerNumber, retryCount);
  }

  private PeriodicControllerChecker openControllerChecker(final IndexerWorkerContext context)
  {
    return PeriodicControllerChecker.open(
        getControllerTaskId(),
        getId(),
        context.controllerLocator(),
        workerRunRef::cancel
    );
  }
}
