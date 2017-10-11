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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.guice.annotations.ExtensionPoint;
import io.druid.indexing.overlord.autoscaling.ec2.EC2AutoScaler;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The AutoScaler has the actual methods to provision and terminate worker nodes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopAutoScaler.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "ec2", value = EC2AutoScaler.class)
})
@ExtensionPoint
public interface AutoScaler<T>
{
  int getMinNumWorkers();

  int getMaxNumWorkers();

  T getEnvConfig();

  @Nullable
  AutoScalingData provision();

  @Nullable
  AutoScalingData terminate(List<String> ips);

  @Nullable
  AutoScalingData terminateWithIds(List<String> ids);

  /**
   * Provides a lookup of ip addresses to node ids
   *
   * @param ips - nodes IPs
   *
   * @return node ids
   */
  List<String> ipToIdLookup(List<String> ips);

  /**
   * Provides a lookup of node ids to ip addresses
   *
   * @param nodeIds - nodes ids
   *
   * @return IPs associated with the node
   */
  List<String> idToIpLookup(List<String> nodeIds);
}
