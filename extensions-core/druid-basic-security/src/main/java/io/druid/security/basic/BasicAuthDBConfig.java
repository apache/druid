/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic;

import io.druid.metadata.PasswordProvider;

public class BasicAuthDBConfig
{
  public static final long DEFAULT_CACHE_NOTIFY_TIMEOUT_MS = 5000;

  private final PasswordProvider initialAdminPassword;
  private final PasswordProvider initialInternalClientPassword;
  private final boolean enableCacheNotifications;
  private final long cacheNotificationTimeout;
  private final int iterations;

  public BasicAuthDBConfig(
      final PasswordProvider initialAdminPassword,
      final PasswordProvider initialInternalClientPassword,
      final Boolean enableCacheNotifications,
      final Long cacheNotificationTimeout,
      final int iterations
  )
  {
    this.initialAdminPassword = initialAdminPassword;
    this.initialInternalClientPassword = initialInternalClientPassword;
    this.enableCacheNotifications = enableCacheNotifications;
    this.cacheNotificationTimeout = cacheNotificationTimeout;
    this.iterations = iterations;
  }

  public PasswordProvider getInitialAdminPassword()
  {
    return initialAdminPassword;
  }

  public PasswordProvider getInitialInternalClientPassword()
  {
    return initialInternalClientPassword;
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
}
