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

import org.apache.druid.msq.indexing.MSQWorkerTask;
import org.apache.druid.msq.indexing.error.MSQFault;

/**
 * Notifies users of {@link WorkerManager} when a worker fails.
 */
public interface WorkerFailureListener
{
  /**
   * Fires when a worker launched or monitoring by {@link WorkerManager} fails.
   */
  void onFailure(MSQWorkerTask workerTask, MSQFault msqFault);
}
