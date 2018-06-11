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

package io.druid.security.basic.authorization.db.cache;

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
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthorizerCacheNotifier implements BasicAuthorizerCacheNotifier
{

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private CommonCacheNotifier cacheNotifier;

  @Inject
  public CoordinatorBasicAuthorizerCacheNotifier(
      AuthorizerMapper authorizerMapper,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    cacheNotifier = new CommonCacheNotifier(
        getAuthorizerConfigMap(authorizerMapper),
        discoveryProvider,
        httpClient,
        "/druid-ext/basic-security/authorization/listen/%s",
        "CoordinatorBasicAuthorizerCacheNotifier"
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

  private Map<String, BasicAuthDBConfig> getAuthorizerConfigMap(AuthorizerMapper mapper)
  {
    Preconditions.checkNotNull(mapper);
    Preconditions.checkNotNull(mapper.getAuthorizerMap());

    Map<String, BasicAuthDBConfig> authorizerConfigMap = new HashMap<>();
    for (Map.Entry<String, Authorizer> entry : mapper.getAuthorizerMap().entrySet()) {
      Authorizer authorizer = entry.getValue();
      if (authorizer instanceof BasicRoleBasedAuthorizer) {
        String authorizerName = entry.getKey();
        BasicRoleBasedAuthorizer basicRoleBasedAuthorizer = (BasicRoleBasedAuthorizer) authorizer;
        BasicAuthDBConfig dbConfig = basicRoleBasedAuthorizer.getDbConfig();
        authorizerConfigMap.put(authorizerName, dbConfig);
      }
    }

    return authorizerConfigMap;
  }
}
