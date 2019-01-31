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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.security.basic.BasicAuthDBConfig;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BasicAuthConfig
{
  private final String url;
  private final String bindUser;
  private final String bindPassword;
  private final String baseDn;
  private final String userSearch;
  private final String userAttribute;
  private final String[] groupFilters;

  public BasicAuthConfig(
      @JsonProperty("url") String url,
      @JsonProperty("bindUser") String bindUser,
      @JsonProperty("bindPassword") String bindPassword,
      @JsonProperty("baseDn") String baseDn,
      @JsonProperty("userSearch") String userSearch,
      @JsonProperty("userAttribute") String userAttribute,
      @JsonProperty("groupFilters") String[] groupFilters
  )
  {
    this.url = url;
    this.bindUser = bindUser;
    this.bindPassword = bindPassword;
    this.baseDn = baseDn;
    this.userSearch = userSearch;
    this.userAttribute = userAttribute;
    this.groupFilters = groupFilters;
  }

  public BasicAuthConfig(BasicAuthDBConfig config)
  {
    this.url = config.getUrl();
    this.bindUser = config.getBindUser();
    this.bindPassword = config.getBindPassword() != null ? config.getBindPassword().getPassword() : null;
    this.baseDn = config.getBaseDn();
    this.userSearch = config.getUserSearch();
    this.userAttribute = config.getUserAttribute();
    this.groupFilters = config.getGroupFilters();
  }

  @JsonProperty
  public String getUrl()
  {
    return url;
  }

  @JsonProperty
  public String getBindUser()
  {
    return bindUser;
  }

  @JsonProperty
  public String getBindPassword()
  {
    return bindPassword;
  }

  @JsonProperty
  public String getBaseDn()
  {
    return baseDn;
  }

  @JsonProperty
  public String getUserSearch()
  {
    return userSearch;
  }

  @JsonProperty
  public String getUserAttribute()
  {
    return userAttribute;
  }

  @JsonProperty
  public String[] getGroupFilters()
  {
    return groupFilters;
  }
}
