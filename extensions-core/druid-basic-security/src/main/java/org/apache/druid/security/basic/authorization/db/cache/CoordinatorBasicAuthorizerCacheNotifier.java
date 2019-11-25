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

package org.apache.druid.security.basic.authorization.db.cache;

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
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthorizerCacheNotifier implements BasicAuthorizerCacheNotifier
{

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final CommonCacheNotifier cacheUserNotifier;
  private final CommonCacheNotifier cacheGroupMappingNotifier;

  @Inject
  public CoordinatorBasicAuthorizerCacheNotifier(
      AuthorizerMapper authorizerMapper,
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient
  )
  {
    cacheUserNotifier = new CommonCacheNotifier(
        getAuthorizerConfigMap(authorizerMapper),
        discoveryProvider,
        httpClient,
        "/druid-ext/basic-security/authorization/listen/users/%s",
        "CoordinatorBasicAuthorizerCacheNotifier"
    );
    cacheGroupMappingNotifier = new CommonCacheNotifier(
        getAuthorizerConfigMap(authorizerMapper),
        discoveryProvider,
        httpClient,
        "/druid-ext/basic-security/authorization/listen/groupMappings/%s",
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
      cacheUserNotifier.start();
      cacheGroupMappingNotifier.start();
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
      cacheUserNotifier.stop();
      cacheGroupMappingNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void addUpdateUser(String updatedAuthorizerPrefix, byte[] userAndRoleMap)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    cacheUserNotifier.addUpdate(updatedAuthorizerPrefix, userAndRoleMap);
  }

  @Override
  public void addUpdateGroupMapping(String updatedAuthorizerPrefix, byte[] groupMappingAndRoleMap)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    cacheGroupMappingNotifier.addUpdate(updatedAuthorizerPrefix, groupMappingAndRoleMap);
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
