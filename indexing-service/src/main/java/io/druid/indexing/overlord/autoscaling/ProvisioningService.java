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

package io.druid.indexing.overlord.autoscaling;

import io.druid.guice.annotations.PublicApi;

import java.io.Closeable;

/**
 * The ProvisioningService decides if worker nodes should be provisioned or terminated
 * based on the available tasks in the system and the state of the workers in the system.
 *
 * ProvisioningService is tied to the task runner.
 *
 * @see ProvisioningStrategy#makeProvisioningService
 */
@PublicApi
public interface ProvisioningService extends Closeable
{
  /**
   * Should be called from TaskRunner's lifecycle stop
   */
  @Override
  void close();

  /**
   * Get any interesting stats related to scaling
   *
   * @return The ScalingStats or `null` if nothing of interest
   */
  ScalingStats getStats();
}
