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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.server.DruidNode;

import java.io.File;

/**
 * Context used by multi-stage query workers.
 *
 * Each context is scoped to a {@link Worker} and is shared across all {@link WorkOrder} run by that worker.
 */
public interface WorkerContext
{
  /**
   * Query ID for this context.
   */
  String queryId();

  /**
   * Identifier for this worker that enables the controller, and other workers, to find it. For tasks this is the
   * task ID from {@link MSQWorkerTask#getId()}. For persistent servers, this is the server URI.
   */
  String workerId();

  ObjectMapper jsonMapper();

  PolicyEnforcer policyEnforcer();

  // Using an Injector directly because tasks do not have a way to provide their own Guice modules.
  Injector injector();

  /**
   * Callback from the worker implementation to "register" the worker. Used in
   * the indexer to set up the task chat services.
   */
  void registerWorker(Worker worker, Closer closer);

  /**
   * Maximum number of {@link WorkOrder} that a {@link Worker} with this context will be asked to execute
   * simultaneously.
   */
  int maxConcurrentStages();

  /**
   * Creates a controller client.
   */
  ControllerClient makeControllerClient();

  /**
   * Creates and fetches a {@link WorkerClient}. It is independent of the workerId because the workerId is passed
   * in to every method of the client.
   */
  WorkerClient makeWorkerClient();

  /**
   * Directory for temporary outputs, used as a base for {@link FrameContext#tempDir()}. This directory is not
   * necessarily fully owned by the worker.
   */
  File tempDir();

  /**
   * Create a context with useful objects required by {@link StageProcessor#makeProcessors}.
   */
  FrameContext frameContext(WorkOrder workOrder);

  /**
   * Number of available processing threads.
   */
  int threadCount();

  /**
   * Fetch node info about self.
   */
  DruidNode selfNode();

  /**
   * Returns the factory for {@link DataServerQueryHandler} from the context. Used to query realtime tasks.
   */
  DataServerQueryHandlerFactory dataServerQueryHandlerFactory();

  /**
   * Whether to include all counters in reports. See {@link MultiStageQueryContext#CTX_INCLUDE_ALL_COUNTERS} for detail.
   */
  boolean includeAllCounters();
}
