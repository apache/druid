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

public class BasicAuthDBConfig
{
  public static final long DEFAULT_CACHE_NOTIFY_TIMEOUT_MS = 5000;

  private final PasswordProvider initialAdminPassword;
  private final PasswordProvider initialInternalClientPassword;
  private final String initialAdminUser;
  private final String initialAdminRole;
  private final String initialAdminGroupMapping;
  private final boolean enableCacheNotifications;
  private final long cacheNotificationTimeout;
  private final int iterations;
  private final String url;
  private final String bindUser;
  private final PasswordProvider bindPassword;
  private final String baseDn;
  private final String userSearch;
  private final String userAttribute;
  private final String[] groupFilters;
  private final Integer credentialVerifyDuration;
  private final Integer credentialMaxDuration;
  private final Integer credentialCacheSize;

  public BasicAuthDBConfig(
      final PasswordProvider initialAdminPassword,
      final PasswordProvider initialInternalClientPassword,
      final String initialAdminUser,
      final String initialAdminRole,
      final String initialAdminGroupMapping,
      final boolean enableCacheNotifications,
      final long cacheNotificationTimeout,
      final int credentialIterations,
      final String url,
      final String bindUser,
      final PasswordProvider bindPassword,
      final String baseDn,
      final String userSearch,
      final String userAttribute,
      final String[] groupFilters,
      final Integer credentialVerifyDuration,
      final Integer credentialMaxDuration,
      final Integer credentialCacheSize
  )
  {
    this.initialAdminPassword = initialAdminPassword;
    this.initialInternalClientPassword = initialInternalClientPassword;
    this.initialAdminUser = initialAdminUser;
    this.initialAdminRole = initialAdminRole;
    this.initialAdminGroupMapping = initialAdminGroupMapping;
    this.enableCacheNotifications = enableCacheNotifications;
    this.cacheNotificationTimeout = cacheNotificationTimeout;
    this.iterations = credentialIterations;
    this.url = url;
    this.bindUser = bindUser;
    this.bindPassword = bindPassword;
    this.baseDn = baseDn;
    this.userSearch = userSearch;
    this.userAttribute = userAttribute;
    this.groupFilters = groupFilters;
    this.credentialVerifyDuration = credentialVerifyDuration;
    this.credentialMaxDuration = credentialMaxDuration;
    this.credentialCacheSize = credentialCacheSize;
  }

  public PasswordProvider getInitialAdminPassword()
  {
    return initialAdminPassword;
  }

  public PasswordProvider getInitialInternalClientPassword()
  {
    return initialInternalClientPassword;
  }

  public String getInitialAdminUser()
  {
    return initialAdminUser;
  }

  public String getInitialAdminRole()
  {
    return initialAdminRole;
  }

  public String getInitialAdminGroupMapping()
  {
    return initialAdminGroupMapping;
  }

  public boolean isEnableCacheNotifications()
  {
    return enableCacheNotifications;
  }

  public long getCacheNotificationTimeout()
  {
    return cacheNotificationTimeout;
  }

  public int getIterations()
  {
    return iterations;
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
