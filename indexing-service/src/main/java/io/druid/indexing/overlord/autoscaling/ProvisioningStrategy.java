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

import io.druid.guice.annotations.ExtensionPoint;
import io.druid.indexing.overlord.TaskRunner;

/**
 * In general, the resource management is tied to the runner.
 */
@ExtensionPoint
public interface ProvisioningStrategy<T extends TaskRunner>
{
  /**
   * Creates a new {@link ProvisioningService} for the given {@link TaskRunner}
   * This method is intended to be called from the TaskRunner's lifecycle start
   *
   * @param runner The TaskRunner state holder this strategy should use during execution
   */
  ProvisioningService makeProvisioningService(T runner);
}
