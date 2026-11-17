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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.security.ResourceAction;

import java.util.List;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BasicAuthorizerRoleFull
{
  private final String name;
  private final Set<String> users;
  private final Set<String> groups;
  private final List<ResourceAction> permissions;

  @JsonCreator
  public BasicAuthorizerRoleFull(
      @JsonProperty("name") String name,
      @JsonProperty("users") Set<String> users,
      @JsonProperty("groups") Set<String> groups,
      @JsonProperty("permissions") List<ResourceAction> permissions
  )
  {
    this.name = name;
    this.users = users;
    this.groups = groups;
    this.permissions = permissions == null ? List.of() : permissions;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<ResourceAction> getPermissions()
  {
    return permissions;
  }

  @JsonProperty
  public Set<String> getUsers()
  {
    return users;
  }

  @JsonProperty
  public Set<String> getGroups()
  {
    return groups;
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

    BasicAuthorizerRoleFull that = (BasicAuthorizerRoleFull) o;

    if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
      return false;
    }
    if (getUsers() != null ? !getUsers().equals(that.getUsers()) : that.getUsers() != null) {
      return false;
    }
    if (getGroups() != null ? !getGroups().equals(that.getGroups()) : that.getGroups() != null) {
      return false;
    }
    return getPermissions() != null ? getPermissions().equals(that.getPermissions()) : that.getPermissions() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 31 * result + (getUsers() != null ? getUsers().hashCode() : 0);
    result = 31 * result + (getGroups() != null ? getGroups().hashCode() : 0);
    result = 31 * result + (getPermissions() != null ? getPermissions().hashCode() : 0);
    return result;
  }
}
