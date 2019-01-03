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

package org.apache.druid.grpc;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class GrpcConfig
{
  @JsonProperty
  @Min(0)
  public int port = 8200;

  @JsonProperty
  @Min(0)
  public long shutdownTimeoutMs = 90000;

  @JsonProperty(required = true)
  public String serviceName;

  public int getPort()
  {
    return port;
  }

  public long getShutdownTimeoutMs()
  {
    return shutdownTimeoutMs;
  }

  public String getServiceName()
  {
    if (serviceName == null) {
      throw new NullPointerException("serviceName");
    }
    return serviceName;
  }
}
