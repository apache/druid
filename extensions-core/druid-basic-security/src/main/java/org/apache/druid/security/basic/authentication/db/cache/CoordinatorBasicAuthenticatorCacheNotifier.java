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

package org.apache.druid.security.basic.authentication.db.cache;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.security.basic.BasicAuthDBConfig;
import org.apache.druid.security.basic.CommonCacheNotifier;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthenticatorCacheNotifier implements BasicAuthenticatorCacheNotifier
{
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CommonCacheNotifier userCacheNotifier;

  @Inject
  public CoordinatorBasicAuthenticatorCacheNotifier(
      AuthenticatorMapper authenticatorMapper,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    userCacheNotifier = new CommonCacheNotifier(
        initAuthenticatorConfigMap(authenticatorMapper),
        discoveryProvider,
        httpClient,
        "/druid-ext/basic-security/authentication/listen/%s",
        "CoordinatorBasicAuthenticatorCacheNotifier"
    );
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("Can't start.");
    }

    try {
      userCacheNotifier.start();
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      return;
    }
    try {
      userCacheNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void addUserUpdate(String updatedAuthenticatorPrefix, byte[] updatedUserMap)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    userCacheNotifier.addUpdate(updatedAuthenticatorPrefix, updatedUserMap);
  }

  private Map<String, BasicAuthDBConfig> initAuthenticatorConfigMap(AuthenticatorMapper mapper)
  {
    Preconditions.checkNotNull(mapper);
    Preconditions.checkNotNull(mapper.getAuthenticatorMap());

    Map<String, BasicAuthDBConfig> authenticatorConfigMap = new HashMap<>();

    for (Map.Entry<String, Authenticator> entry : mapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
        authenticatorConfigMap.put(authenticatorName, dbConfig);
      }
    }

    return authenticatorConfigMap;
  }
}
