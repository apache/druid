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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.authentication.BasicHTTPEscalator;
import org.apache.druid.security.basic.authentication.BytesFullResponseHandler;
import org.apache.druid.security.basic.authentication.BytesFullResponseHolder;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;
import org.apache.druid.server.security.Escalator;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache manager for non-coordinator services that polls the coordinator for escalator database state.
 */
@ManageLifecycle
public class CoordinatorPollingBasicEscalatorCacheManager implements BasicEscalatorCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingBasicEscalatorCacheManager.class);

  private final AtomicReference<BasicEscalatorCredential> cachedEscalatorCredential;
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidLeaderClient druidLeaderClient;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ScheduledExecutorService exec;

  @Inject
  public CoordinatorPollingBasicEscalatorCacheManager(
      Injector injector,
      BasicAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.exec = Execs.scheduledSingleThreaded("BasicAuthenticatorCacheManager-Exec--%d");
    this.injector = injector;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.druidLeaderClient = druidLeaderClient;
    this.cachedEscalatorCredential = new AtomicReference<>();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("Can't start.");
    }

    LOG.info("Starting CoordinatorPollingBasicEscalatorCacheManager.");

    try {
      initEscalatorCredential();

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting cachedConfigs random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled escalator credential cache poll is running");
              BasicEscalatorCredential escalatorCredential = fetchEscalatorCredentialFromCoordinator();
              if (escalatorCredential != null) {
                cachedEscalatorCredential.set(escalatorCredential);
              }
              LOG.debug("Scheduled escalator credential  cache poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedEscalatorCredential.").emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Started CoordinatorPollingBasicEscalatorCacheManager.");
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    LOG.info("CoordinatorPollingBasicEscalatorCacheManager is stopping.");
    exec.shutdown();
    LOG.info("CoordinatorPollingBasicEscalatorCacheManager is stopped.");
  }

  @Override
  public void handleEscalatorCredentialUpdate(byte[] serializedEscalatorCredentialConfig)
  {
    LOG.debug("Received escalator credential cache update for escalator.");
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {
      cachedEscalatorCredential.set(
          objectMapper.readValue(serializedEscalatorCredentialConfig, BasicEscalatorCredential.class)
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize escalator credential received from coordinator.").emit();
    }
  }

  @Override
  public BasicEscalatorCredential getEscalatorCredential()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return cachedEscalatorCredential.get();
  }

  @Nullable
  private BasicEscalatorCredential fetchEscalatorCredentialFromCoordinator()
  {
    try {
      return RetryUtils.retry(
          this::tryFetchEscalatorCredentialFromCoordinator,
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching escalator credential for escalator").emit();
      return null;
    }
  }

  private BasicEscalatorCredential tryFetchEscalatorCredentialFromCoordinator() throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        "/druid-ext/basic-security/escalator/db/cachedSerializedCredential"
    );
    BytesFullResponseHolder responseHolder = (BytesFullResponseHolder) druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    byte[] escalatorCredentialBytes = responseHolder.getBytes();

    return objectMapper.readValue(escalatorCredentialBytes, BasicEscalatorCredential.class);
  }

  private void initEscalatorCredential()
  {
    Escalator escalator = injector.getInstance(Escalator.class);

    if (escalator == null) {
      return;
    }

    if (escalator instanceof BasicHTTPEscalator) {
      BasicEscalatorCredential escalatorCredential = fetchEscalatorCredentialFromCoordinator();
      if (escalatorCredential != null) {
        cachedEscalatorCredential.set(escalatorCredential);
      }
    }
  }
}
