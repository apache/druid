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

package org.apache.druid.discovery;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Metadata of a service announced by node. See DataNodeService and LookupNodeService for examples.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = DataNodeService.DISCOVERY_SERVICE_KEY, value = DataNodeService.class),
    @JsonSubTypes.Type(name = LookupNodeService.DISCOVERY_SERVICE_KEY, value = LookupNodeService.class),
    @JsonSubTypes.Type(name = WorkerNodeService.DISCOVERY_SERVICE_KEY, value = WorkerNodeService.class)
})
public abstract class DruidService
{
  public abstract String getName();

  /**
   * @return Whether the service should be discoverable. The default implementation returns true.
   *
   * Some implementations may choose to override this so that the service is not discoverable if it has not been
   * configured. This will not throw a fatal exception, but instead will just skip binding and log a message. This could
   * be useful for optional configuration for the service.
   */
  @JsonIgnore
  public boolean isDiscoverable()
  {
    return true;
  }
}
