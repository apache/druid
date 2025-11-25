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

import com.google.inject.ImplementedBy;

/**
 * Provides identifying information for a running task. Implementations may throw an exception in
 * non-task contexts, so callers must check the implementation type before invoking its methods.
 *
 * <p>Note: {@link ImplementedBy} is used so that a default implementation is
 * available without requiring explicit bindings in non-task server processes
 * and various tests.</p>
 */
@ImplementedBy(NoopTaskHolder.class)
public interface TaskHolder
{
  /**
   * @return the datasource name for the task.
   */
  String getDataSource();

  /**
   * @return the unique task ID.
   */
  String getTaskId();
}
