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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import io.druid.client.coordinator.Coordinator;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DruidLeaderClient;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.security.basic.BasicAuthCommonCacheConfig;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.authentication.BytesFullResponseHandler;
import io.druid.security.basic.authentication.BytesFullResponseHolder;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import io.druid.security.basic.authorization.entity.UserAndRoleMap;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
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

@ManageLifecycle
public class CoordinatorPollingBasicAuthorizerCacheManager implements BasicAuthorizerCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorPollingBasicAuthorizerCacheManager.class);

  private final ConcurrentHashMap<String, Map<String, BasicAuthorizerUser>> cachedUserMaps;
  private final ConcurrentHashMap<String, Map<String, BasicAuthorizerRole>> cachedRoleMaps;

  private final Set<String> authorizerPrefixes;
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidLeaderClient druidLeaderClient;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ScheduledExecutorService exec;

  @Inject
  public CoordinatorPollingBasicAuthorizerCacheManager(
      Injector injector,
      BasicAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorPollingBasicAuthorizerCacheManager-Exec--%d");
    this.injector = injector;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.cachedRoleMaps = new ConcurrentHashMap<>();
    this.authorizerPrefixes = new HashSet<>();
    this.druidLeaderClient = druidLeaderClient;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    LOG.info("Starting CoordinatorPollingBasicAuthorizerCacheManager.");

    try {
      initUserMaps();

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled cache poll is running");
              for (String authorizerPrefix : authorizerPrefixes) {
                UserAndRoleMap userAndRoleMap = fetchUserAndRoleMapFromCoordinator(authorizerPrefix, false);
                if (userAndRoleMap != null) {
                  cachedUserMaps.put(authorizerPrefix, userAndRoleMap.getUserMap());
                  cachedRoleMaps.put(authorizerPrefix, userAndRoleMap.getRoleMap());
                }
              }
              LOG.debug("Scheduled cache poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedUserMaps.").emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Started CoordinatorPollingBasicAuthorizerCacheManager.");
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

    LOG.info("CoordinatorPollingBasicAuthorizerCacheManager is stopping.");
    exec.shutdown();
    LOG.info("CoordinatorPollingBasicAuthorizerCacheManager is stopped.");
  }

  @Override
  public void handleAuthorizerUpdate(String authorizerPrefix, byte[] serializedUserAndRoleMap)
  {
    LOG.debug("Received cache update for authorizer [%s].", authorizerPrefix);
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {
      UserAndRoleMap userAndRoleMap = objectMapper.readValue(
          serializedUserAndRoleMap,
          BasicAuthUtils.AUTHORIZER_USER_AND_ROLE_MAP_TYPE_REFERENCE
      );

      cachedUserMaps.put(authorizerPrefix, userAndRoleMap.getUserMap());
      cachedRoleMaps.put(authorizerPrefix, userAndRoleMap.getRoleMap());

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeMapToDisk(authorizerPrefix, serializedUserAndRoleMap);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "WTF? Could not deserialize user/role map received from coordinator.").emit();
    }
  }

  @Override
  public Map<String, BasicAuthorizerUser> getUserMap(String authorizerPrefix)
  {
    return cachedUserMaps.get(authorizerPrefix);
  }

  @Override
  public Map<String, BasicAuthorizerRole> getRoleMap(String authorizerPrefix)
  {
    return cachedRoleMaps.get(authorizerPrefix);
  }

  private String getUserRoleMapFilename(String prefix)
  {
    return StringUtils.format("%s.authorizer.cache", prefix);
  }

  @Nullable
  private UserAndRoleMap loadUserAndRoleMapFromDisk(String prefix) throws IOException
  {
    File userAndRoleMapFile = new File(commonCacheConfig.getCacheDirectory(), getUserRoleMapFilename(prefix));
    if (!userAndRoleMapFile.exists()) {
      return null;
    }
    return objectMapper.readValue(
        userAndRoleMapFile,
        BasicAuthUtils.AUTHORIZER_USER_AND_ROLE_MAP_TYPE_REFERENCE
    );
  }

  private void writeMapToDisk(String prefix, byte[] userMapBytes) throws IOException
  {
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    cacheDir.mkdirs();
    File userMapFile = new File(commonCacheConfig.getCacheDirectory(), getUserRoleMapFilename(prefix));
    Files.write(userMapBytes, userMapFile);
  }

  @Nullable
  private UserAndRoleMap fetchUserAndRoleMapFromCoordinator(String prefix, boolean isInit)
  {
    try {
      return RetryUtils.retry(
          () -> {
            return tryFetchMapsFromCoordinator(prefix);
          },
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching user and role map for authorizer [%s]", prefix);
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load user map snapshot from disk.");
            return loadUserAndRoleMapFromDisk(prefix);
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading user-role map snapshot for authorizer [%s]", prefix);
          }
        }
      }
      return null;
    }
  }

  private UserAndRoleMap tryFetchMapsFromCoordinator(
      String prefix
  ) throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid-ext/basic-security/authorization/db/%s/cachedSerializedUserMap", prefix)
    );
    BytesFullResponseHolder responseHolder = (BytesFullResponseHolder) druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    byte[] userRoleMapBytes = responseHolder.getBytes();

    UserAndRoleMap userAndRoleMap = objectMapper.readValue(
        userRoleMapBytes,
        BasicAuthUtils.AUTHORIZER_USER_AND_ROLE_MAP_TYPE_REFERENCE
    );
    if (userAndRoleMap != null && commonCacheConfig.getCacheDirectory() != null) {
      writeMapToDisk(prefix, userRoleMapBytes);
    }
    return userAndRoleMap;
  }

  private void initUserMaps()
  {
    AuthorizerMapper authorizerMapper = injector.getInstance(AuthorizerMapper.class);

    if (authorizerMapper == null || authorizerMapper.getAuthorizerMap() == null) {
      return;
    }

    for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
      Authorizer authorizer = entry.getValue();
      if (authorizer instanceof BasicRoleBasedAuthorizer) {
        String authorizerName = entry.getKey();
        authorizerPrefixes.add(authorizerName);
        UserAndRoleMap userAndRoleMap = fetchUserAndRoleMapFromCoordinator(authorizerName, true);
        if (userAndRoleMap != null) {
          cachedUserMaps.put(authorizerName, userAndRoleMap.getUserMap());
          cachedRoleMaps.put(authorizerName, userAndRoleMap.getRoleMap());
        }
      }
    }
  }
}
