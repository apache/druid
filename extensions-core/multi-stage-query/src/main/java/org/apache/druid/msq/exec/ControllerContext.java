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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.server.DruidNode;

import java.util.Map;

/**
 * Context used by multi-stage query controllers.
 *
 * Useful because it allows test fixtures to provide their own implementations.
 */
public interface ControllerContext
{
  ObjectMapper jsonMapper();

  /**
   * Provides a way for tasks to request injectable objects. Useful because tasks are not able to request injection
   * at the time of server startup, because the server doesn't know what tasks it will be running.
   */
  Injector injector();

  /**
   * Fetch node info about self.
   */
  DruidNode selfNode();

  /**
   * Provide access to the Coordinator service.
   */
  CoordinatorClient coordinatorClient();

  /**
   * Provide access to segment actions in the Overlord.
   */
  TaskActionClient taskActionClient();

  /**
   * Provides services about workers: starting, canceling, obtaining status.
   */
  WorkerManagerClient workerManager();

  /**
   * Callback from the controller implementation to "register" the controller. Used in the indexing task implementation
   * to set up the task chat web service.
   */
  void registerController(Controller controller, Closer closer);

  /**
   * Client for communicating with workers.
   */
  WorkerClient taskClientFor(Controller controller);

  /**
   * Writes controller task report.
   */
  void writeReports(String controllerTaskId, Map<String, TaskReport> reports);
}
