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

package io.druid.server.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ResourceAction
{
  private final Resource resource;
  private final Action action;

  @JsonCreator
  public ResourceAction(
      @JsonProperty("resource") Resource resource,
      @JsonProperty("action") Action action
  )
  {
    this.resource = resource;
    this.action = action;
  }

  @JsonProperty
  public Resource getResource()
  {
    return resource;
  }

  @JsonProperty
  public Action getAction()
  {
    return action;
  }
}
