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

package org.apache.druid.security.basic;

import org.apache.druid.metadata.PasswordProvider;

import java.util.List;
import java.util.Map;

public class BasicAuthLdapConfig
{
  private final String url;
  private final String bindUser;
  private final PasswordProvider bindPassword;
  private final String baseDn;
  private final String userSearch;
  private final String userAttribute;
  private final String[] groupFilters;
  private final Map<String, List<String>> groupMappings;
  private final Integer credentialIterations;
  private final Integer credentialVerifyDuration;
  private final Integer credentialMaxDuration;
  private final Integer credentialCacheSize;

  public BasicAuthLdapConfig(
      final String url,
      final String bindUser,
      final PasswordProvider bindPassword,
      final String baseDn,
      final String userSearch,
      final String userAttribute,
      final String[] groupFilters,
      final Map<String, List<String>> groupMappings,
      final Integer credentialIterations,
      Integer credentialVerifyDuration,
      Integer credentialMaxDuration,
      Integer credentialCacheSize
  )
  {
    this.url = url;
    this.bindUser = bindUser;
    this.bindPassword = bindPassword;
    this.baseDn = baseDn;
    this.userSearch = userSearch;
    this.userAttribute = userAttribute;
    this.groupFilters = groupFilters;
    this.groupMappings = groupMappings;
    this.credentialIterations = credentialIterations;
    this.credentialVerifyDuration = credentialVerifyDuration;
    this.credentialMaxDuration = credentialMaxDuration;
    this.credentialCacheSize = credentialCacheSize;
  }

  public String getUrl()
  {
    return url;
  }

  public String getBindUser()
  {
    return bindUser;
  }

  public PasswordProvider getBindPassword()
  {
    return bindPassword;
  }

  public String getBaseDn()
  {
    return baseDn;
  }

  public String getUserSearch()
  {
    return userSearch;
  }

  public String getUserAttribute()
  {
    return userAttribute;
  }

  public String[] getGroupFilters()
  {
    return groupFilters;
  }

  public Map<String, List<String>> getGroupMappings()
  {
    return groupMappings;
  }

  public Integer getCredentialIterations()
  {
    return credentialIterations;
  }

  public Integer getCredentialVerifyDuration()
  {
    return credentialVerifyDuration;
  }

  public Integer getCredentialMaxDuration()
  {
    return credentialMaxDuration;
  }

  public Integer getCredentialCacheSize()
  {
    return credentialCacheSize;
  }
}
