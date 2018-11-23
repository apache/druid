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

package org.apache.druid.security.basic.authorization.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class BasicAuthorizerRole
{
  private final String name;
  private final List<BasicAuthorizerPermission> permissions;

  @JsonCreator
  public BasicAuthorizerRole(
      @JsonProperty("name") String name,
      @JsonProperty("permissions") List<BasicAuthorizerPermission> permissions
  )
  {
    this.name = name;
    this.permissions = permissions == null ? new ArrayList<>() : permissions;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<BasicAuthorizerPermission> getPermissions()
  {
    return permissions;
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

    BasicAuthorizerRole role = (BasicAuthorizerRole) o;

    if (getName() != null ? !getName().equals(role.getName()) : role.getName() != null) {
      return false;
    }
    return getPermissions() != null ? getPermissions().equals(role.getPermissions()) : role.getPermissions() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 31 * result + (getPermissions() != null ? getPermissions().hashCode() : 0);
    return result;
  }
}
