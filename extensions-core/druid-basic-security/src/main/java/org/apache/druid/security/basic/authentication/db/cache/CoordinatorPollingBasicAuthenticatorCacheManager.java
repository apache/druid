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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Cache manager for non-coordinator services that polls the coordinator for authentication database state.
 */
@ManageLifecycle
public class CoordinatorPollingBasicAuthenticatorCacheManager implements BasicAuthenticatorCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingBasicAuthenticatorCacheManager.class);

  private final ConcurrentHashMap<String, Map<String, BasicAuthenticatorUser>> cachedUserMaps;
  private final Set<String> authenticatorPrefixes;
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidLeaderClient druidLeaderClient;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ScheduledExecutorService exec;

  @Inject
  public CoordinatorPollingBasicAuthenticatorCacheManager(
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
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.authenticatorPrefixes = new HashSet<>();
    this.druidLeaderClient = druidLeaderClient;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    LOG.info("Starting CoordinatorPollingBasicAuthenticatorCacheManager.");

    try {
      initUserMaps();

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting cachedUserMaps random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled user cache poll is running");
              for (String authenticatorPrefix : authenticatorPrefixes) {
                Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorPrefix, false);
                if (userMap != null) {
                  cachedUserMaps.put(authenticatorPrefix, userMap);
                }
              }
              LOG.debug("Scheduled user cache poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedUserMaps.").emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Started CoordinatorPollingBasicAuthenticatorCacheManager.");
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

    LOG.info("CoordinatorPollingBasicAuthenticatorCacheManager is stopping.");
    exec.shutdown();
    LOG.info("CoordinatorPollingBasicAuthenticatorCacheManager is stopped.");
  }

  @Override
  public void handleAuthenticatorUserMapUpdate(String authenticatorPrefix, byte[] serializedUserMap)
  {
    LOG.debug("Received user cache update for authenticator [%s].", authenticatorPrefix);
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {
      cachedUserMaps.put(
          authenticatorPrefix,
          objectMapper.readValue(
              serializedUserMap,
              BasicAuthUtils.AUTHENTICATOR_USER_MAP_TYPE_REFERENCE
          )
      );

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeUserMapToDisk(authenticatorPrefix, serializedUserMap);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize user map received from coordinator.").emit();
    }
  }

  @Override
  public Map<String, BasicAuthenticatorUser> getUserMap(String authenticatorPrefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return cachedUserMaps.get(authenticatorPrefix);
  }

  @Nullable
  private Map<String, BasicAuthenticatorUser> fetchUserMapFromCoordinator(String prefix, boolean isInit)
  {
    try {
      return RetryUtils.retry(
          () -> {
            return tryFetchUserMapFromCoordinator(prefix);
          },
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching user map for authenticator [%s]", prefix).emit();
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load user map snapshot from disk.");
            return loadUserMapFromDisk(prefix);
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading user map snapshot for authenticator [%s]", prefix)
               .emit();
          }
        }
      }
      return null;
    }
  }

  private String getUserMapFilename(String prefix)
  {
    return StringUtils.format("%s.authenticator.cache", prefix);
  }

  @Nullable
  private Map<String, BasicAuthenticatorUser> loadUserMapFromDisk(String prefix) throws IOException
  {
    File userMapFile = new File(commonCacheConfig.getCacheDirectory(), getUserMapFilename(prefix));
    if (!userMapFile.exists()) {
      return null;
    }
    return objectMapper.readValue(
        userMapFile,
        BasicAuthUtils.AUTHENTICATOR_USER_MAP_TYPE_REFERENCE
    );
  }

  private void writeUserMapToDisk(String prefix, byte[] userMapBytes) throws IOException
  {
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    cacheDir.mkdirs();
    File userMapFile = new File(commonCacheConfig.getCacheDirectory(), getUserMapFilename(prefix));
    FileUtils.writeAtomically(
        userMapFile,
        out -> {
          out.write(userMapBytes);
          return null;
        }
    );
  }

  private Map<String, BasicAuthenticatorUser> tryFetchUserMapFromCoordinator(String prefix) throws Exception
  {
    Map<String, BasicAuthenticatorUser> userMap = null;
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid-ext/basic-security/authentication/db/%s/cachedSerializedUserMap", prefix)
    );
    BytesFullResponseHolder responseHolder = druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    byte[] userMapBytes = responseHolder.getContent();
    if (ArrayUtils.isNotEmpty(userMapBytes)) {
      userMap = objectMapper.readValue(
          userMapBytes,
          BasicAuthUtils.AUTHENTICATOR_USER_MAP_TYPE_REFERENCE
      );
      if (userMap != null && commonCacheConfig.getCacheDirectory() != null) {
        writeUserMapToDisk(prefix, userMapBytes);
      }
    } else {
      LOG.info("Empty cached serialized user map retrieved, authenticator - %s", prefix);
    }
    return userMap;
  }

  private void initUserMaps()
  {
    AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    if (authenticatorMapper == null || authenticatorMapper.getAuthenticatorMap() == null) {
      return;
    }

    for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        authenticatorPrefixes.add(authenticatorName);

        Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorName, true);
        if (userMap != null) {
          cachedUserMaps.put(authenticatorName, userMap);
        }
      }
    }
  }
}
