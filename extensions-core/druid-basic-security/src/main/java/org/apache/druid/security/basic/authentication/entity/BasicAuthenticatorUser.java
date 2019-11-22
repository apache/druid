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

package org.apache.druid.security.basic.authentication.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicAuthenticatorUser
{
  private final String name;
  private final BasicAuthenticatorCredentials credentials;

  @JsonCreator
  public BasicAuthenticatorUser(
      @JsonProperty("name") String name,
      @JsonProperty("credentials") BasicAuthenticatorCredentials credentials
  )
  {
    this.name = name;
    this.credentials = credentials;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public BasicAuthenticatorCredentials getCredentials()
  {
    return credentials;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }

    BasicAuthenticatorUser that = (BasicAuthenticatorUser) o;

    return (getName() != null ? getName().equals(that.getName()) : that.getName() == null)
           && (getCredentials() != null ? getCredentials().equals(that.getCredentials()) : that.getCredentials() == null);
  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 31 * result + (getCredentials() != null ? getCredentials().hashCode() : 0);
    return result;
  }
}
