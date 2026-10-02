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

package org.apache.druid.security.opa.opatypes;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.security.AuthenticationResult;

public class OpaInput
{
  private final AuthenticationResult authenticationResult;
  private final String action;
  private final OpaResource resource;

  public OpaInput(
      @JsonProperty("authenticationResult") AuthenticationResult authenticationResult,
      @JsonProperty("action") String action,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceType") String resourceType
  )
  {
    this.authenticationResult = authenticationResult;
    this.action = action;
    this.resource = new OpaResource(resourceName, resourceType);
  }

  @JsonProperty
  public AuthenticationResult getAuthenticationResult()
  {
    return authenticationResult;
  }

  @JsonProperty
  public String getAction()
  {
    return action;
  }

  @JsonProperty
  public OpaResource getResource()
  {
    return resource;
  }
}
