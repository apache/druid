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

package io.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.guice.annotations.ExtensionPoint;

/**
 * Outside of {@link io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerProvisioningStrategy} and
 * {@link io.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningStrategy}, WorkerBehaviorConfig is used only
 * in {@link io.druid.indexing.overlord.TaskRunner}, and only {@link #getSelectStrategy()} method is used. That is why
 * the WorkerBehaviorConfig's interface is minimized to just this method. PendingTaskBasedWorkerProvisioningStrategy and
 * SimpleWorkerProvisioningStrategy are written to work only with {@link DefaultWorkerBehaviorConfig}. Extension-defined
 * WorkerBehaviorConfig implementations should likely be used with different extension-defined {@link
 * io.druid.indexing.overlord.autoscaling.ProvisioningStrategy} implementations as well.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultWorkerBehaviorConfig.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultWorkerBehaviorConfig.class)
})
@ExtensionPoint
public interface WorkerBehaviorConfig
{
  String CONFIG_KEY = "worker.config";
  WorkerSelectStrategy DEFAULT_STRATEGY = new EqualDistributionWorkerSelectStrategy(null);

  WorkerSelectStrategy getSelectStrategy();
}
