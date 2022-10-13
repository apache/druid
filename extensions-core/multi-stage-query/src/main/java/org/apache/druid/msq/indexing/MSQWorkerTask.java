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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Injector;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerImpl;

import java.util.Map;

@JsonTypeName(MSQWorkerTask.TYPE)
public class MSQWorkerTask extends AbstractTask
{
  public static final String TYPE = "query_worker";

  private final String controllerTaskId;
  private final int workerNumber;

  // Using an Injector directly because tasks do not have a way to provide their own Guice modules.
  @JacksonInject
  private Injector injector;

  private volatile Worker worker;

  @JsonCreator
  @VisibleForTesting
  public MSQWorkerTask(
      @JsonProperty("controllerTaskId") final String controllerTaskId,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("workerNumber") final int workerNumber,
      @JsonProperty("context") final Map<String, Object> context
  )
  {
    super(
        MSQTasks.workerTaskId(controllerTaskId, workerNumber),
        controllerTaskId,
        null,
        dataSource,
        context
    );

    this.controllerTaskId = controllerTaskId;
    this.workerNumber = workerNumber;
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

  @Override
  public String getType()
  {
    return TYPE;
  }


  @Override
  public boolean isReady(final TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final WorkerContext context = IndexerWorkerContext.createProductionInstance(toolbox, injector);
    worker = new WorkerImpl(this, context);
    return worker.run();
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {
    if (worker != null) {
      worker.stopGracefully();
    }
  }
}
