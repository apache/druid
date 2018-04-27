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
import io.druid.java.util.http.client.HttpClient;

/**
 * This interface provides methods needed for escalating internal system requests with priveleged authentication
 * credentials. Each Escalator is associated with a specific authentication scheme, like Authenticators.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopEscalator.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "noop", value = NoopEscalator.class),
})
public interface Escalator
{
  /**
   * Return a client that sends requests with the format/information necessary to authenticate successfully
   * against this Escalator's authentication scheme using the identity of the internal system user.
   * <p>
   * This HTTP client is used for internal communications between Druid nodes, such as when a broker communicates
   * with a historical node during query processing.
   *
   * @param baseClient Base HTTP client for internal Druid communications
   *
   * @return metamx HttpClient that sends requests with the credentials of the internal system user
   */
  HttpClient createEscalatedClient(HttpClient baseClient);

  /**
   * @return an AuthenticationResult representing the identity of the internal system user.
   */
  AuthenticationResult createEscalatedAuthenticationResult();

}
