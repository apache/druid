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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerManagerClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.server.DruidNode;

import java.util.Map;

/**
 * Implementation for {@link ControllerContext} required to run multi-stage queries as indexing tasks.
 */
public class IndexerControllerContext implements ControllerContext
{
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final ServiceClientFactory clientFactory;
  private final OverlordClient overlordClient;
  private final WorkerManagerClient workerManager;

  public IndexerControllerContext(
      final TaskToolbox toolbox,
      final Injector injector,
      final ServiceClientFactory clientFactory,
      final OverlordClient overlordClient
  )
  {
    this.toolbox = toolbox;
    this.injector = injector;
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
    this.workerManager = new IndexerWorkerManagerClient(overlordClient);
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return toolbox.getJsonMapper();
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public DruidNode selfNode()
  {
    return injector.getInstance(Key.get(DruidNode.class, Self.class));
  }

  @Override
  public CoordinatorClient coordinatorClient()
  {
    return toolbox.getCoordinatorClient();
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    return toolbox.getTaskActionClient();
  }

  @Override
  public WorkerClient taskClientFor(Controller controller)
  {
    // Ignore controller parameter.
    return new IndexerWorkerClient(clientFactory, overlordClient, jsonMapper());
  }

  @Override
  public void registerController(Controller controller, final Closer closer)
  {
    ChatHandler chatHandler = new ControllerChatHandler(toolbox, controller);
    toolbox.getChatHandlerProvider().register(controller.id(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(controller.id()));
  }

  @Override
  public WorkerManagerClient workerManager()
  {
    return workerManager;
  }

  @Override
  public void writeReports(String controllerTaskId, Map<String, TaskReport> reports)
  {
    toolbox.getTaskReportFileWriter().write(controllerTaskId, reports);
  }
}
