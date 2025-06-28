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

package org.apache.druid.segment.realtime.appenderator;

import java.io.File;

/**
 * Contains the paths for various directories used by a task for logs, reports,
 * and persisting intermediate data.
 */
public interface TaskDirectory
{
  /**
   * @return {@code {baseTaskDir}/{taskId}}
   */
  File getTaskDir(String taskId);

  /**
   * @return {@code {baseTaskDir}/{taskId}/work}
   */
  File getTaskWorkDir(String taskId);

  /**
   * @return {@code {baseTaskDir}/{taskId}/log}
   */
  File getTaskLogFile(String taskId);

  /**
   * @return {@code {baseTaskDir}/{taskId}/temp}
   */
  File getTaskTempDir(String taskId);

  /**
   * @return {@code {baseTaskDir}/{taskId}/lock}
   */
  File getTaskLockFile(String taskId);
}
