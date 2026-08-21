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

import javax.annotation.Nullable;

public class BasicAuthLDAPConfig
{
  private final String url;
  private final String bindUser;
  private final PasswordProvider bindPassword;
  private final String baseDn;
  private final String userSearch;
  private final String userAttribute;
  private final int credentialIterations;
  private final Integer credentialVerifyDuration;
  private final Integer credentialMaxDuration;
  private final Integer credentialCacheSize;
  @Nullable
  private final String groupBaseDn;
  @Nullable
  private final String groupSearch;

  public BasicAuthLDAPConfig(
      final String url,
      final String bindUser,
      final PasswordProvider bindPassword,
      final String baseDn,
      final String userSearch,
      final String userAttribute,
      final int credentialIterations,
      final Integer credentialVerifyDuration,
      final Integer credentialMaxDuration,
      final Integer credentialCacheSize
  )
  {
    this(
        url,
        bindUser,
        bindPassword,
        baseDn,
        userSearch,
        userAttribute,
        credentialIterations,
        credentialVerifyDuration,
        credentialMaxDuration,
        credentialCacheSize,
        null,
        null
    );
  }

  public BasicAuthLDAPConfig(
      final String url,
      final String bindUser,
      final PasswordProvider bindPassword,
      final String baseDn,
      final String userSearch,
      final String userAttribute,
      final int credentialIterations,
      final Integer credentialVerifyDuration,
      final Integer credentialMaxDuration,
      final Integer credentialCacheSize,
      @Nullable final String groupBaseDn,
      @Nullable final String groupSearch
  )
  {
    this.url = url;
    this.bindUser = bindUser;
    this.bindPassword = bindPassword;
    this.baseDn = baseDn;
    this.userSearch = userSearch;
    this.userAttribute = userAttribute;
    this.credentialIterations = credentialIterations;
    this.credentialVerifyDuration = credentialVerifyDuration;
    this.credentialMaxDuration = credentialMaxDuration;
    this.credentialCacheSize = credentialCacheSize;
    this.groupBaseDn = groupBaseDn;
    this.groupSearch = groupSearch;
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

  public int getCredentialIterations()
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

  @Nullable
  public String getGroupBaseDn()
  {
    return groupBaseDn;
  }

  @Nullable
  public String getGroupSearch()
  {
    return groupSearch;
  }

  public boolean isGroupSearchConfigured()
  {
    return groupBaseDn != null && groupSearch != null;
  }
}
