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

package io.druid.security.basic.authentication.db.cache;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.http.client.HttpClient;
import io.druid.security.basic.BasicAuthDBConfig;
import io.druid.security.basic.CommonCacheNotifier;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthenticatorCacheNotifier implements BasicAuthenticatorCacheNotifier
{

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private CommonCacheNotifier cacheNotifier;

  @Inject
  public CoordinatorBasicAuthenticatorCacheNotifier(
      AuthenticatorMapper authenticatorMapper,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    cacheNotifier = new CommonCacheNotifier(
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
      throw new ISE("can't start.");
    }

    try {
      cacheNotifier.start();
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
      cacheNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void addUpdate(String updatedAuthorizerPrefix, byte[] updatedUserMap)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    cacheNotifier.addUpdate(updatedAuthorizerPrefix, updatedUserMap);
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
