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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultAuthorizationManager.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultAuthorizationManager.class),
    @JsonSubTypes.Type(name = "noop", value = NoopAuthorizationManager.class)
})
public interface AuthorizationManager
{
  /**
   * Check if the entity represented by `identity` in `namespace` is authorized to perform `action` on `resource`.
   *
   * @param identity The identity of the requester
   * @param namespace The namespace of the identity
   * @param resource The resource to be accessed
   * @param action The action to perform on the resource
   * @return An Access object representing the result of the authorization check.
   */
  public Access authorize(String identity, Resource resource, Action action);

  /**
   * @return The namespace associated with this AuthorizationManager. Authenticator implementations will
   * put the namespace in request headers.
   */
  public String getNamespace();
}
