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

public class AuthConfig
{
  /**
   * Use this String as the attribute name for the request attribute to pass {@link AuthorizationInfo}
   * from the servlet filter to the jersey resource
   * */
  public static final String DRUID_AUTH_TOKEN = "Druid-Auth-Token";

  public AuthConfig()
  {
    this(false);
  }

  @JsonCreator
  public AuthConfig(@JsonProperty("enabled") boolean enabled)
  {
    this.enabled = enabled;
  }
  /**
   * If druid.auth.enabled is set to true then an implementation of AuthorizationInfo
   * must be provided and it must be set as a request attribute possibly inside the servlet filter
   * injected in the filter chain using your own extension
   * */
  @JsonProperty
  private final boolean enabled;

  public boolean isEnabled()
  {
    return enabled;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuthConfig that = (AuthConfig) o;

    return enabled == that.enabled;

  }

  @Override
  public int hashCode()
  {
    return (enabled ? 1 : 0);
  }

  @Override
  public String toString()
  {
    return "AuthConfig{" +
           "enabled=" + enabled +
           '}';
  }
}
