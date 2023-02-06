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
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.server.DruidNode;

import java.io.File;

/**
 * Context used by multi-stage query workers.
 *
 * Useful because it allows test fixtures to provide their own implementations.
 */
public interface WorkerContext
{
  ObjectMapper jsonMapper();

  // Using an Injector directly because tasks do not have a way to provide their own Guice modules.
  Injector injector();

  /**
   * Callback from the worker implementation to "register" the worker. Used in
   * the indexer to set up the task chat services.
   */
  void registerWorker(Worker worker, Closer closer);

  /**
   * Creates and fetches the controller client for the provided controller ID.
   */
  ControllerClient makeControllerClient(String controllerId);

  /**
   * Creates and fetches a {@link WorkerClient}. It is independent of the workerId because the workerId is passed
   * in to every method of the client.
   */
  WorkerClient makeWorkerClient();

  /**
   * Fetch a directory for temporary outputs
   */
  File tempDir();

  FrameContext frameContext(QueryDefinition queryDef, int stageNumber);

  int threadCount();

  /**
   * Fetch node info about self
   */
  DruidNode selfNode();

  Bouncer processorBouncer();
}
