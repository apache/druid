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

/**
 * Expanded {@link WorkerManager} interface with methods to support retrying workers.
 */
public interface RetryCapableWorkerManager extends WorkerManager
{
  /**
   * Queues worker for relaunch. A noop if the worker is already in the queue.
   */
  void submitForRelaunch(int workerNumber);

  /**
   * Report a worker that failed without active orders. To be retried if it is requried for future stages only.
   */
  void reportFailedInactiveWorker(int workerNumber);

  /**
   * Checks if the controller has canceled the input taskId. This method is used in {@link ControllerImpl}
   * to figure out if the worker taskId is canceled by the controller. If yes, the errors from that worker taskId
   * are ignored for the error reports.
   *
   * @return true if task is canceled by the controller, else false
   */
  boolean isTaskCanceledByController(String taskId);
}
