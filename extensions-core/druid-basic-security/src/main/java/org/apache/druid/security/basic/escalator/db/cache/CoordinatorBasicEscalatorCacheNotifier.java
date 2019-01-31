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

package org.apache.druid.security.basic.escalator.db.cache;

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
import org.apache.druid.security.basic.authentication.BasicHTTPEscalator;
import org.apache.druid.server.security.Escalator;

import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicEscalatorCacheNotifier implements BasicEscalatorCacheNotifier
{

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private CommonCacheNotifier escalatorCredentialCacheNotifier;

  @Inject
  public CoordinatorBasicEscalatorCacheNotifier(
      DruidNodeDiscoveryProvider discoveryProvider,
      final Escalator escalator,
      @EscalatedClient HttpClient httpClient
  )
  {
    escalatorCredentialCacheNotifier = new CommonCacheNotifier(
        initEscalatorConfigMap(escalator),
        discoveryProvider,
        httpClient,
        "/druid-ext/basic-security/escalator/listen/credential",
        "CoordinatorBasicEscalatorCacheNotifier"
    );
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      escalatorCredentialCacheNotifier.start();
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
      escalatorCredentialCacheNotifier.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void addEscalatorCredentialUpdate(byte[] updatedEscalatorCredential)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    escalatorCredentialCacheNotifier.addUpdate(updatedEscalatorCredential);
  }

  private BasicAuthDBConfig initEscalatorConfigMap(Escalator escalator)
  {
    Preconditions.checkNotNull(escalator);

    BasicAuthDBConfig dbConfig;
    if (escalator instanceof BasicHTTPEscalator) {
      BasicHTTPEscalator basicHTTPEscalator = (BasicHTTPEscalator) escalator;
      dbConfig = basicHTTPEscalator.getDbConfig();
    } else {
      dbConfig = new BasicAuthDBConfig(
          null,
          null,
          null,
          null,
          null,
          true,
          BasicAuthDBConfig.DEFAULT_CACHE_NOTIFY_TIMEOUT_MS,
          0,
          null,
          null,
          null,
          null,
          null,
          null,
          null, null,
          null,
          null
      );
    }
    return dbConfig;
  }
}
