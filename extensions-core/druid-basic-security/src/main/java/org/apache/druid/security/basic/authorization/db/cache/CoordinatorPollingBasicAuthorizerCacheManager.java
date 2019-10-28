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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
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
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.security.basic.authorization.entity.GroupMappingAndRoleMap;
import org.apache.druid.security.basic.authorization.entity.UserAndRoleMap;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
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
  private final ConcurrentHashMap<String, Map<String, BasicAuthorizerGroupMapping>> cachedGroupMappingMaps;
  private final ConcurrentHashMap<String, Map<String, BasicAuthorizerRole>> cachedGroupMappingRoleMaps;

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
    this.cachedGroupMappingMaps = new ConcurrentHashMap<>();
    this.cachedGroupMappingRoleMaps = new ConcurrentHashMap<>();
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

              LOG.debug("Scheduled userMap cache poll is running");
              for (String authorizerPrefix : authorizerPrefixes) {
                UserAndRoleMap userAndRoleMap = fetchUserAndRoleMapFromCoordinator(authorizerPrefix, false);
                if (userAndRoleMap != null) {
                  cachedUserMaps.put(authorizerPrefix, userAndRoleMap.getUserMap());
                  cachedRoleMaps.put(authorizerPrefix, userAndRoleMap.getRoleMap());
                }
              }
              LOG.debug("Scheduled userMap cache poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedUserMaps.").emit();
            }
          }
      );

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.debug("Inserting random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);

              LOG.debug("Scheduled groupMappingMap cache poll is running");
              for (String authorizerPrefix : authorizerPrefixes) {
                GroupMappingAndRoleMap groupMappingAndRoleMap = fetchGroupAndRoleMapFromCoordinator(authorizerPrefix, false);
                if (groupMappingAndRoleMap != null) {
                  cachedGroupMappingMaps.put(authorizerPrefix, groupMappingAndRoleMap.getGroupMappingMap());
                  cachedGroupMappingRoleMaps.put(authorizerPrefix, groupMappingAndRoleMap.getRoleMap());
                }
              }
              LOG.debug("Scheduled groupMappingMap cache poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedGroupMappingMaps.").emit();
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
  public void handleAuthorizerUserUpdate(String authorizerPrefix, byte[] serializedUserAndRoleMap)
  {
    LOG.debug("Received userMap cache update for authorizer [%s].", authorizerPrefix);
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {
      UserAndRoleMap userAndRoleMap = objectMapper.readValue(
          serializedUserAndRoleMap,
          BasicAuthUtils.AUTHORIZER_USER_AND_ROLE_MAP_TYPE_REFERENCE
      );

      cachedUserMaps.put(authorizerPrefix, userAndRoleMap.getUserMap());
      cachedRoleMaps.put(authorizerPrefix, userAndRoleMap.getRoleMap());

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeUserMapToDisk(authorizerPrefix, serializedUserAndRoleMap);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "WTF? Could not deserialize user/role map received from coordinator.").emit();
    }
  }

  @Override
  public void handleAuthorizerGroupMappingUpdate(String authorizerPrefix, byte[] serializedGroupMappingAndRoleMap)
  {
    LOG.debug("Received groupMappingMap cache update for authorizer [%s].", authorizerPrefix);
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    try {
      GroupMappingAndRoleMap groupMappingAndRoleMap = objectMapper.readValue(
          serializedGroupMappingAndRoleMap,
          BasicAuthUtils.AUTHORIZER_GROUP_MAPPING_AND_ROLE_MAP_TYPE_REFERENCE
      );

      cachedGroupMappingMaps.put(authorizerPrefix, groupMappingAndRoleMap.getGroupMappingMap());
      cachedGroupMappingRoleMaps.put(authorizerPrefix, groupMappingAndRoleMap.getRoleMap());

      if (commonCacheConfig.getCacheDirectory() != null) {
        writeGroupMappingMapToDisk(authorizerPrefix, serializedGroupMappingAndRoleMap);
      }
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Could not deserialize groupMapping/role map received from coordinator.").emit();
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

  @Override
  public Map<String, BasicAuthorizerGroupMapping> getGroupMappingMap(String authorizerPrefix)
  {
    return cachedGroupMappingMaps.get(authorizerPrefix);
  }

  @Override
  public Map<String, BasicAuthorizerRole> getGroupMappingRoleMap(String authorizerPrefix)
  {
    return cachedGroupMappingRoleMaps.get(authorizerPrefix);
  }

  private String getUserRoleMapFilename(String prefix)
  {
    return StringUtils.format("%s.authorizer.userRole.cache", prefix);
  }

  private String getGroupMappingRoleMapFilename(String prefix)
  {
    return StringUtils.format("%s.authorizer.groupMappingRole.cache", prefix);
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

  @Nullable
  private GroupMappingAndRoleMap loadGroupMappingAndRoleMapFromDisk(String prefix) throws IOException
  {
    File groupMappingAndRoleMapFile = new File(commonCacheConfig.getCacheDirectory(), getGroupMappingRoleMapFilename(prefix));
    if (!groupMappingAndRoleMapFile.exists()) {
      return null;
    }
    return objectMapper.readValue(
        groupMappingAndRoleMapFile,
        BasicAuthUtils.AUTHORIZER_GROUP_MAPPING_AND_ROLE_MAP_TYPE_REFERENCE
    );
  }

  private void writeUserMapToDisk(String prefix, byte[] userMapBytes) throws IOException
  {
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    cacheDir.mkdirs();
    File userMapFile = new File(commonCacheConfig.getCacheDirectory(), getUserRoleMapFilename(prefix));
    FileUtils.writeAtomically(
        userMapFile,
        out -> {
          out.write(userMapBytes);
          return null;
        }
    );
  }

  private void writeGroupMappingMapToDisk(String prefix, byte[] groupMappingBytes) throws IOException
  {
    File cacheDir = new File(commonCacheConfig.getCacheDirectory());
    cacheDir.mkdirs();
    File groupMapFile = new File(commonCacheConfig.getCacheDirectory(), getGroupMappingRoleMapFilename(prefix));
    FileUtils.writeAtomically(
        groupMapFile,
        out -> {
          out.write(groupMappingBytes);
          return null;
        }
    );
  }

  @Nullable
  private UserAndRoleMap fetchUserAndRoleMapFromCoordinator(String prefix, boolean isInit)
  {
    try {
      return RetryUtils.retry(
          () -> {
            return tryFetchUserMapsFromCoordinator(prefix);
          },
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching user and role map for authorizer [%s]", prefix).emit();
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load user map snapshot from disk.");
            return loadUserAndRoleMapFromDisk(prefix);
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading user-role map snapshot for authorizer [%s]", prefix)
               .emit();
          }
        }
      }
      return null;
    }
  }

  @Nullable
  private GroupMappingAndRoleMap fetchGroupAndRoleMapFromCoordinator(String prefix, boolean isInit)
  {
    try {
      return RetryUtils.retry(
          () -> {
            return tryFetchGroupMappingMapsFromCoordinator(prefix);
          },
          e -> true,
          commonCacheConfig.getMaxSyncRetries()
      );
    }
    catch (Exception e) {
      LOG.makeAlert(e, "Encountered exception while fetching group and role map for authorizer [%s]", prefix).emit();
      if (isInit) {
        if (commonCacheConfig.getCacheDirectory() != null) {
          try {
            LOG.info("Attempting to load group map snapshot from disk.");
            return loadGroupMappingAndRoleMapFromDisk(prefix);
          }
          catch (Exception e2) {
            e2.addSuppressed(e);
            LOG.makeAlert(e2, "Encountered exception while loading group-role map snapshot for authorizer [%s]", prefix)
               .emit();
          }
        }
      }
      return null;
    }
  }

  private UserAndRoleMap tryFetchUserMapsFromCoordinator(
      String prefix
  ) throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid-ext/basic-security/authorization/db/%s/cachedSerializedUserMap", prefix)
    );
    BytesFullResponseHolder responseHolder = druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    byte[] userRoleMapBytes = responseHolder.getContent();

    UserAndRoleMap userAndRoleMap = objectMapper.readValue(
        userRoleMapBytes,
        BasicAuthUtils.AUTHORIZER_USER_AND_ROLE_MAP_TYPE_REFERENCE
    );
    if (userAndRoleMap != null && commonCacheConfig.getCacheDirectory() != null) {
      writeUserMapToDisk(prefix, userRoleMapBytes);
    }
    return userAndRoleMap;
  }

  private GroupMappingAndRoleMap tryFetchGroupMappingMapsFromCoordinator(
      String prefix
  ) throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid-ext/basic-security/authorization/db/%s/cachedSerializedGroupMappingMap", prefix)
    );
    BytesFullResponseHolder responseHolder = druidLeaderClient.go(
        req,
        new BytesFullResponseHandler()
    );
    byte[] groupRoleMapBytes = responseHolder.getContent();

    GroupMappingAndRoleMap groupMappingAndRoleMap = objectMapper.readValue(
        groupRoleMapBytes,
        BasicAuthUtils.AUTHORIZER_GROUP_MAPPING_AND_ROLE_MAP_TYPE_REFERENCE
    );
    if (groupMappingAndRoleMap != null && commonCacheConfig.getCacheDirectory() != null) {
      writeGroupMappingMapToDisk(prefix, groupRoleMapBytes);
    }
    return groupMappingAndRoleMap;
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

        GroupMappingAndRoleMap groupMappingAndRoleMap = fetchGroupAndRoleMapFromCoordinator(authorizerName, true);
        if (groupMappingAndRoleMap != null) {
          cachedGroupMappingMaps.put(authorizerName, groupMappingAndRoleMap.getGroupMappingMap());
          cachedGroupMappingRoleMaps.put(authorizerName, groupMappingAndRoleMap.getRoleMap());
        }
      }
    }
  }
}
