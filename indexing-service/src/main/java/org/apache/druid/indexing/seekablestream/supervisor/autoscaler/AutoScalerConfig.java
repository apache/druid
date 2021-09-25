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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;

@UnstableApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "autoScalerStrategy", defaultImpl = LagBasedAutoScalerConfig.class)
@JsonSubTypes(value = {
        @Type(name = "lagBased", value = LagBasedAutoScalerConfig.class)
})
public interface AutoScalerConfig
{
  boolean getEnableTaskAutoScaler();
  long getMinTriggerScaleActionFrequencyMillis();
  int getTaskCountMax();
  int getTaskCountMin();
  SupervisorTaskAutoScaler createAutoScaler(Supervisor supervisor, SupervisorSpec spec);
}

