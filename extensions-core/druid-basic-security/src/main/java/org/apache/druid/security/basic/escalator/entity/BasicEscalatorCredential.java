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

package org.apache.druid.security.basic.escalator.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BasicEscalatorCredential
{
  private final String username;
  private final String password;

  public BasicEscalatorCredential(
      @JsonProperty("username") String username,
      @JsonProperty("password") String password
  )
  {
    this.username = username;
    this.password = password;
  }

  @JsonProperty
  public String getUsername()
  {
    return username;
  }

  @JsonProperty
  public String getPassword()
  {
    return password;
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

    BasicEscalatorCredential that = (BasicEscalatorCredential) o;

    if (getUsername() != null ? !getUsername().equals(that.getUsername()) : that.getUsername() != null) {
      return false;
    }
    return getPassword() != null ? getPassword().equals(that.getPassword()) : that.getPassword() == null;
  }

  @Override
  public int hashCode()
  {
    int result = getUsername() != null ? getUsername().hashCode() : 0;
    result = 31 * result + (getPassword() != null ? getPassword().hashCode() : 0);
    return result;
  }
}
