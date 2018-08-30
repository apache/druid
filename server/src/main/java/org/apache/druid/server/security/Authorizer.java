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

package org.apache.druid.server.security;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = AuthConfig.ALLOW_ALL_NAME, value = AllowAllAuthorizer.class)
})
/**
 * An Authorizer is responsible for performing authorization checks for resource accesses.
 *
 * A single instance of each Authorizer implementation will be created per node.
 * Security-sensitive endpoints will need to extract the identity string contained in the request's Druid-Auth-Token
 * attribute, previously set by an Authenticator. Each endpoint will pass this identity String to the
 * Authorizer's authorize() method along with any Resource/Action pairs created for the request being
 * handled. The endpoint can use these checks to filter out resources or deny the request as needed.
 * After a request is authorized, a new attribute, "Druid-Authorization-Checked", should be set in the
 * request header with the result of the authorization decision.
 */
public interface Authorizer
{
  /**
   * Check if the entity represented by {@code identity} is authorized to perform {@code action} on {@code resource}.
   *
   * @param authenticationResult  The authentication result of the request
   * @param resource  The resource to be accessed
   * @param action    The action to perform on the resource
   *
   * @return An Access object representing the result of the authorization check. Must not be null.
   */
  Access authorize(AuthenticationResult authenticationResult, Resource resource, Action action);
}
