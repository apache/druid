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

package org.apache.druid.server.metrics;

/**
 * Proides task / task count status at the level of individual worker nodes. These merics
 * are repoerted by workers, like middle-managers.
 */
public interface WorkerTaskCountStatsProvider
{
  /**
   * The number of failed tasks run on worker during emission period.
   */
  Long getWorkerFailedTaskCount();

  /**
   * The number of successful tasks run on worker during emission period.
   */
  Long getWorkerSuccessfulTaskCount();

  /**
   * The number of idle task slots on worker.
   */
  Long getWorkerIdleTaskSlotCount();

  /**
   * The number of total task slots on worker.
   */
  Long getWorkerTotalTaskSlotCount();

  /**
   * The number of used task slots on worker.
   */
  Long getWorkerUsedTaskSlotCount();


  /**
   * The worker category.
   */
  String getWorkerCategory();

  /**
   * The worker version.
   */
  String getWorkerVersion();
}
