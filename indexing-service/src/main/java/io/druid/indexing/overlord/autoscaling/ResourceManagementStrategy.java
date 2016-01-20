/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord.autoscaling;

import io.druid.indexing.overlord.TaskRunner;

/**
 * The ResourceManagementStrategy decides if worker nodes should be provisioned or determined
 * based on the available tasks in the system and the state of the workers in the system.
 * In general, the resource management is tied to the runner.
 */
public interface ResourceManagementStrategy<T extends TaskRunner>
{
  /**
   * Equivalent to start() but requires a specific runner instance which holds state of interest.
   * This method is intended to be called from the TaskRunner's lifecycle
   *
   * @param runner The TaskRunner state holder this strategy should use during execution
   */
  void startManagement(T runner);

  /**
   * Equivalent to stop()
   * Should be called from TaskRunner's lifecycle
   */
  void stopManagement();

  /**
   * Get any interesting stats related to scaling
   *
   * @return The ScalingStats or `null` if nothing of interest
   */
  ScalingStats getStats();
}
