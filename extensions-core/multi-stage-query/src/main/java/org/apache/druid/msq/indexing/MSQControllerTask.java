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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@JsonTypeName(MSQControllerTask.TYPE)
public class MSQControllerTask extends AbstractTask
{
  public static final String TYPE = "query_controller";
  public static final String DUMMY_DATASOURCE_FOR_SELECT = "__query_select";

  private final MSQSpec querySpec;

  // Enables users, and the web console, to see the original SQL query (if any). Not used by anything else in Druid.
  @Nullable
  private final String sqlQuery;

  // Enables users, and the web console, to see the original SQL context (if any). Not used by any other Druid logic.
  @Nullable
  private final Map<String, Object> sqlQueryContext;

  // Enables users, and the web console, to see the original SQL type names (if any). Not used by any other Druid logic.
  @Nullable
  private final List<String> sqlTypeNames;
  @Nullable
  private final ExecutorService remoteFetchExecutorService;

  // Using an Injector directly because tasks do not have a way to provide their own Guice modules.
  @JacksonInject
  private Injector injector;

  private volatile Controller controller;

  @JsonCreator
  public MSQControllerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("spec") MSQSpec querySpec,
      @JsonProperty("sqlQuery") @Nullable String sqlQuery,
      @JsonProperty("sqlQueryContext") @Nullable Map<String, Object> sqlQueryContext,
      @JsonProperty("sqlTypeNames") @Nullable List<String> sqlTypeNames,
      @JsonProperty("context") @Nullable Map<String, Object> context
  )
  {
    super(
        id != null ? id : MSQTasks.controllerTaskId(null),
        id,
        null,
        getDataSourceForTaskMetadata(querySpec),
        context
    );

    this.querySpec = querySpec;
    this.sqlQuery = sqlQuery;
    this.sqlQueryContext = sqlQueryContext;
    this.sqlTypeNames = sqlTypeNames;

    if (MultiStageQueryContext.isDurableStorageEnabled(querySpec.getQuery().context())) {
      this.remoteFetchExecutorService =
          Executors.newCachedThreadPool(Execs.makeThreadFactory(getId() + "-remote-fetcher-%d"));
    } else {
      this.remoteFetchExecutorService = null;
    }

    addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty("spec")
  public MSQSpec getQuerySpec()
  {
    return querySpec;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSqlQuery()
  {
    return sqlQuery;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Object> getSqlQueryContext()
  {
    return sqlQueryContext;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    // If we're in replace mode, acquire locks for all intervals before declaring the task ready.
    if (isIngestion(querySpec) && ((DataSourceMSQDestination) querySpec.getDestination()).isReplaceTimeChunks()) {
      final List<Interval> intervals =
          ((DataSourceMSQDestination) querySpec.getDestination()).getReplaceTimeChunks();

      for (final Interval interval : intervals) {
        final TaskLock taskLock =
            taskActionClient.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval));

        if (taskLock == null) {
          return false;
        } else if (taskLock.isRevoked()) {
          throw new ISE(StringUtils.format("Lock for interval [%s] was revoked", interval));
        }
      }
    }

    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final ServiceClientFactory clientFactory =
        injector.getInstance(Key.get(ServiceClientFactory.class, EscalatedGlobal.class));
    final OverlordClient overlordClient = injector.getInstance(OverlordClient.class)
                                                  .withRetryPolicy(StandardRetryPolicy.unlimited());
    final ControllerContext context = new IndexerControllerContext(
        toolbox,
        injector,
        clientFactory,
        overlordClient
    );
    controller = new ControllerImpl(this, context);
    return controller.run();
  }

  @Override
  public void stopGracefully(final TaskConfig taskConfig)
  {
    if (controller != null) {
      controller.stopGracefully();
    }
    if (remoteFetchExecutorService != null) {
      // This is to make sure we don't leak connections.
      remoteFetchExecutorService.shutdownNow();
    }
  }

  private static String getDataSourceForTaskMetadata(final MSQSpec querySpec)
  {
    final MSQDestination destination = querySpec.getDestination();

    if (destination instanceof DataSourceMSQDestination) {
      return ((DataSourceMSQDestination) destination).getDataSource();
    } else {
      return DUMMY_DATASOURCE_FOR_SELECT;
    }
  }

  public static boolean isIngestion(final MSQSpec querySpec)
  {
    return querySpec.getDestination() instanceof DataSourceMSQDestination;
  }
}
