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

import java.util.Map;

public class GroupMappingAndRoleMap
{
  @JsonProperty
  private Map<String, BasicAuthorizerGroupMapping> groupMappingMap;

  @JsonProperty
  private Map<String, BasicAuthorizerRole> roleMap;

  @JsonCreator
  public GroupMappingAndRoleMap(
      @JsonProperty("groupMappingMap") Map<String, BasicAuthorizerGroupMapping> groupMappingMap,
      @JsonProperty("roleMap") Map<String, BasicAuthorizerRole> roleMap
  )
  {
    this.groupMappingMap = groupMappingMap;
    this.roleMap = roleMap;
  }

  @JsonProperty
  public Map<String, BasicAuthorizerGroupMapping> getGroupMappingMap()
  {
    return groupMappingMap;
  }

  @JsonProperty
  public Map<String, BasicAuthorizerRole> getRoleMap()
  {
    return roleMap;
  }
}
