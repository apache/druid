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

import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.msq.indexing.MSQWorkerTask;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

/**
 * Generic interface to the "worker manager" mechanism which starts, cancels and monitors worker tasks.
 */
public interface WorkerManagerClient extends Closeable
{
  String run(String controllerId, MSQWorkerTask task);

  /**
   * @param workerId the task ID
   *
   * @return a {@code TaskLocation} associated with the task or
   * {@code TaskLocation.unknown()} if no associated entry could be found
   */
  TaskLocation location(String workerId);

  /**
   * Fetches status map corresponding to a group of task ids
   */
  Map<String, TaskStatus> statuses(Set<String> taskIds);

  /**
   * Cancel the task corresponding to the provided workerId
   */
  void cancel(String workerId);

  @Override
  void close();
}
