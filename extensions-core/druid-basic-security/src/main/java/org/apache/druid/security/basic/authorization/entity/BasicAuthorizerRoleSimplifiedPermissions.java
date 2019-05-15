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
import com.google.common.collect.Lists;
import org.apache.druid.server.security.ResourceAction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The POST API for setting permissions on a role represents permissions as ResourceAction options.
 *
 * However, the BasicAuthorizerRole object in the metadata store has a different representation for permissions, where
 * the permission object keeps a compiled regex pattern for matching.
 *
 * If we return this role object directly, it is somewhat inconvenient for callers of the API. The returned permissions
 * format does not match the permissions format for update APIs. If the client (e.g. a UI for editing roles) has a
 * workflow where the client retrieves existing roles, edits them, and resubmits for updates, this imposes
 * extra work on the client.
 *
 * This class is used to return role information with permissions represented as ResourceAction objects to match
 * the update APIs.
 *
 * The compiled regex pattern is not useful for users, so the response formats that contain the permission+regex are
 * now deprecated. In the future user-facing APIs should only return role information with the simplfied permissions
 * format.
 */
public class BasicAuthorizerRoleSimplifiedPermissions
{
  private final String name;
  private final Set<String> users;
  private final List<ResourceAction> permissions;

  @JsonCreator
  public BasicAuthorizerRoleSimplifiedPermissions(
      @JsonProperty("name") String name,
      @JsonProperty("users") Set<String> users,
      @JsonProperty("permissions") List<ResourceAction> permissions
  )
  {
    this.name = name;
    this.users = users;
    this.permissions = permissions == null ? new ArrayList<>() : permissions;
  }

  public BasicAuthorizerRoleSimplifiedPermissions(
      BasicAuthorizerRole role,
      Set<String> users
  )
  {
    this(
        role.getName(),
        users,
        convertPermissions(role.getPermissions())
    );
  }

  public BasicAuthorizerRoleSimplifiedPermissions(
      BasicAuthorizerRoleFull role
  )
  {
    this(
        role.getName(),
        role.getUsers(),
        convertPermissions(role.getPermissions())
    );
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BasicAuthorizerRoleSimplifiedPermissions that = (BasicAuthorizerRoleSimplifiedPermissions) o;

    if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) {
      return false;
    }
    if (getUsers() != null ? !getUsers().equals(that.getUsers()) : that.getUsers() != null) {
      return false;
    }
    return getPermissions() != null ? getPermissions().equals(that.getPermissions()) : that.getPermissions() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getName() != null ? getName().hashCode() : 0;
    result = 31 * result + (getUsers() != null ? getUsers().hashCode() : 0);
    result = 31 * result + (getPermissions() != null ? getPermissions().hashCode() : 0);
    return result;
  }

  public static List<ResourceAction> convertPermissions(
      List<BasicAuthorizerPermission> permissions
  )
  {
    return Lists.transform(
        permissions,
        (permission) -> {
          return permission.getResourceAction();
        }
    );
  }

  public static Set<BasicAuthorizerRoleSimplifiedPermissions> convertRoles(
      Set<BasicAuthorizerRole> roles
  )
  {
    final HashSet<BasicAuthorizerRoleSimplifiedPermissions> newRoles = new HashSet<>();
    for (BasicAuthorizerRole role : roles) {
      newRoles.add(
          new BasicAuthorizerRoleSimplifiedPermissions(role, null)
      );
    }
    return newRoles;
  }
}
