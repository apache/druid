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

package org.apache.druid.security.basic.authorization.db.updater;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataCASUpdate;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.security.basic.BasicAuthCommonCacheConfig;
import org.apache.druid.security.basic.BasicAuthDBConfig;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheNotifier;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMappingMapBundle;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRoleMapBundle;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUserMapBundle;
import org.apache.druid.security.basic.authorization.entity.GroupMappingAndRoleMap;
import org.apache.druid.security.basic.authorization.entity.UserAndRoleMap;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.joda.time.Duration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthorizerMetadataStorageUpdater implements BasicAuthorizerMetadataStorageUpdater
{
  private static final EmittingLogger LOG =
      new EmittingLogger(CoordinatorBasicAuthorizerMetadataStorageUpdater.class);

  private static final long UPDATE_RETRY_DELAY = 1000;

  private static final String USERS = "users";
  private static final String GROUP_MAPPINGS = "groupMappings";
  private static final String ROLES = "roles";

  public static final List<ResourceAction> SUPERUSER_PERMISSIONS = makeSuperUserPermissions();

  private final AuthorizerMapper authorizerMapper;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final BasicAuthorizerCacheNotifier cacheNotifier;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ObjectMapper objectMapper;
  private final int numRetries = 5;

  private final Map<String, BasicAuthorizerUserMapBundle> cachedUserMaps;
  private final Map<String, BasicAuthorizerGroupMappingMapBundle> cachedGroupMappingMaps;
  private final Map<String, BasicAuthorizerRoleMapBundle> cachedRoleMaps;

  private final Set<String> authorizerNames;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ScheduledExecutorService exec;
  private volatile boolean stopped = false;

  @Inject
  public CoordinatorBasicAuthorizerMetadataStorageUpdater(
      AuthorizerMapper authorizerMapper,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      BasicAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      BasicAuthorizerCacheNotifier cacheNotifier,
      ConfigManager configManager // -V6022: ConfigManager creates the db table we need, set a dependency here
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorBasicAuthorizerMetadataStorageUpdater-Exec--%d");
    this.authorizerMapper = authorizerMapper;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cacheNotifier = cacheNotifier;
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.cachedGroupMappingMaps = new ConcurrentHashMap<>();
    this.cachedRoleMaps = new ConcurrentHashMap<>();
    this.authorizerNames = new HashSet<>();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    if (authorizerMapper == null || authorizerMapper.getAuthorizerMap() == null) {
      return;
    }

    try {
      LOG.info("Starting CoordinatorBasicAuthorizerMetadataStorageUpdater");
      for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
        Authorizer authorizer = entry.getValue();
        if (authorizer instanceof BasicRoleBasedAuthorizer) {
          BasicRoleBasedAuthorizer basicRoleBasedAuthorizer = (BasicRoleBasedAuthorizer) authorizer;
          BasicAuthDBConfig dbConfig = basicRoleBasedAuthorizer.getDbConfig();
          String authorizerName = entry.getKey();
          authorizerNames.add(authorizerName);

          byte[] userMapBytes = getCurrentUserMapBytes(authorizerName);
          Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
              objectMapper,
              userMapBytes
          );
          cachedUserMaps.put(authorizerName, new BasicAuthorizerUserMapBundle(userMap, userMapBytes));

          byte[] groupMappingMapBytes = getCurrentGroupMappingMapBytes(authorizerName);
          Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
              objectMapper,
              groupMappingMapBytes
          );
          cachedGroupMappingMaps.put(authorizerName, new BasicAuthorizerGroupMappingMapBundle(groupMappingMap, groupMappingMapBytes));

          byte[] roleMapBytes = getCurrentRoleMapBytes(authorizerName);
          Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
              objectMapper,
              roleMapBytes
          );
          cachedRoleMaps.put(authorizerName, new BasicAuthorizerRoleMapBundle(roleMap, roleMapBytes));

          initSuperUsersAndGroupMapping(authorizerName, userMap, roleMap, groupMappingMap,
                                        dbConfig.getInitialAdminUser(),
                                        dbConfig.getInitialAdminRole(),
                                        dbConfig.getInitialAdminGroupMapping()
          );
        }
      }

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            if (stopped) {
              return ScheduledExecutors.Signal.STOP;
            }
            try {
              LOG.debug("Scheduled db poll is running");
              for (String authorizerName : authorizerNames) {

                byte[] userMapBytes = getCurrentUserMapBytes(authorizerName);
                Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
                    objectMapper,
                    userMapBytes
                );
                if (userMapBytes != null) {
                  synchronized (cachedUserMaps) {
                    cachedUserMaps.put(authorizerName, new BasicAuthorizerUserMapBundle(userMap, userMapBytes));
                  }
                }

                byte[] groupMappingMapBytes = getCurrentGroupMappingMapBytes(authorizerName);
                Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
                    objectMapper,
                    groupMappingMapBytes
                );
                if (groupMappingMapBytes != null) {
                  synchronized (cachedGroupMappingMaps) {
                    cachedGroupMappingMaps.put(authorizerName, new BasicAuthorizerGroupMappingMapBundle(groupMappingMap, groupMappingMapBytes));
                  }
                }

                byte[] roleMapBytes = getCurrentRoleMapBytes(authorizerName);
                Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
                    objectMapper,
                    roleMapBytes
                );
                if (roleMapBytes != null) {
                  synchronized (cachedRoleMaps) {
                    cachedRoleMaps.put(authorizerName, new BasicAuthorizerRoleMapBundle(roleMap, roleMapBytes));
                  }
                }
              }
              LOG.debug("Scheduled db poll is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedUserMaps, cachedGroupMappingMaps, cachedRoleMaps.").emit();
            }
            return ScheduledExecutors.Signal.REPEAT;
          }
      );

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
      throw new ISE("can't stop.");
    }

    LOG.info("CoordinatorBasicAuthorizerMetadataStorageUpdater is stopping.");
    stopped = true;
    LOG.info("CoordinatorBasicAuthorizerMetadataStorageUpdater is stopped.");
  }


  private static String getPrefixedKeyColumn(String keyPrefix, String keyName)
  {
    return StringUtils.format("basic_authorization_%s_%s", keyPrefix, keyName);
  }

  private boolean tryUpdateUserMap(
      String prefix,
      Map<String, BasicAuthorizerUser> userMap,
      byte[] oldUserMapValue,
      byte[] newUserMapValue
  )
  {
    try {
      List<MetadataCASUpdate> updates = new ArrayList<>();
      if (userMap != null) {
        updates.add(
            createMetadataCASUpdate(prefix, oldUserMapValue, newUserMapValue, USERS)
        );

        boolean succeeded = connector.compareAndSwap(updates);
        if (succeeded) {
          cachedUserMaps.put(prefix, new BasicAuthorizerUserMapBundle(userMap, newUserMapValue));

          byte[] serializedUserAndRoleMap = getCurrentUserAndRoleMapSerialized(prefix);
          cacheNotifier.addUpdateUser(prefix, serializedUserAndRoleMap);

          return true;
        } else {
          return false;
        }
      }
      return false;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean tryUpdateGroupMappingMap(
      String prefix,
      Map<String, BasicAuthorizerGroupMapping> groupMappingMap,
      byte[] oldGroupMappingMapValue,
      byte[] newGroupMappingMapValue
  )
  {
    try {
      List<MetadataCASUpdate> updates = new ArrayList<>();
      if (groupMappingMap != null) {
        updates.add(
            createMetadataCASUpdate(prefix, oldGroupMappingMapValue, newGroupMappingMapValue, GROUP_MAPPINGS)
        );


        boolean succeeded = connector.compareAndSwap(updates);
        if (succeeded) {
          cachedGroupMappingMaps.put(prefix,
                                     new BasicAuthorizerGroupMappingMapBundle(
                                         groupMappingMap,
                                         newGroupMappingMapValue
                                     )
          );

          byte[] serializedGroupMappingAndRoleMap = getCurrentGroupMappingAndRoleMapSerialized(prefix);
          cacheNotifier.addUpdateGroupMapping(prefix, serializedGroupMappingAndRoleMap);

          return true;
        } else {
          return false;
        }
      }
      return false;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean tryUpdateRoleMap(
      String prefix,
      Map<String, BasicAuthorizerRole> roleMap,
      byte[] oldRoleMapValue,
      byte[] newRoleMapValue
  )
  {
    try {
      List<MetadataCASUpdate> updates = new ArrayList<>();
      if (roleMap != null) {
        updates.add(
            createMetadataCASUpdate(prefix, oldRoleMapValue, newRoleMapValue, ROLES)
        );

        boolean succeeded = connector.compareAndSwap(updates);
        if (succeeded) {

          cachedRoleMaps.put(prefix, new BasicAuthorizerRoleMapBundle(roleMap, newRoleMapValue));

          byte[] serializedUserAndRoleMap = getCurrentUserAndRoleMapSerialized(prefix);
          cacheNotifier.addUpdateUser(prefix, serializedUserAndRoleMap);
          byte[] serializedGroupMappingAndRoleMap = getCurrentGroupMappingAndRoleMapSerialized(prefix);
          cacheNotifier.addUpdateGroupMapping(prefix, serializedGroupMappingAndRoleMap);

          return true;
        } else {
          return false;
        }
      }
      return false;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean tryUpdateUserAndRoleMap(
      String prefix,
      Map<String, BasicAuthorizerUser> userMap,
      byte[] oldUserMapValue,
      byte[] newUserMapValue,
      Map<String, BasicAuthorizerRole> roleMap,
      byte[] oldRoleMapValue,
      byte[] newRoleMapValue
  )
  {
    try {
      List<MetadataCASUpdate> updates = new ArrayList<>();
      if (userMap != null && roleMap != null) {
        updates.add(
            createMetadataCASUpdate(prefix, oldUserMapValue, newUserMapValue, USERS)
        );
        updates.add(
            createMetadataCASUpdate(prefix, oldRoleMapValue, newRoleMapValue, ROLES)
        );

        boolean succeeded = connector.compareAndSwap(updates);
        if (succeeded) {
          cachedUserMaps.put(prefix, new BasicAuthorizerUserMapBundle(userMap, newUserMapValue));
          cachedRoleMaps.put(prefix, new BasicAuthorizerRoleMapBundle(roleMap, newRoleMapValue));

          byte[] serializedUserAndRoleMap = getCurrentUserAndRoleMapSerialized(prefix);
          cacheNotifier.addUpdateUser(prefix, serializedUserAndRoleMap);

          return true;
        } else {
          return false;
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    return false;
  }

  private boolean tryUpdateGroupMappingAndRoleMap(
      String prefix,
      Map<String, BasicAuthorizerGroupMapping> groupMappingMap,
      byte[] oldGroupMappingMapValue,
      byte[] newGroupMappingMapValue,
      Map<String, BasicAuthorizerRole> roleMap,
      byte[] oldRoleMapValue,
      byte[] newRoleMapValue
  )
  {
    try {
      List<MetadataCASUpdate> updates = new ArrayList<>();
      if (groupMappingMap != null && roleMap != null) {
        updates.add(
            createMetadataCASUpdate(prefix, oldGroupMappingMapValue, newGroupMappingMapValue, GROUP_MAPPINGS)
        );
        updates.add(
            createMetadataCASUpdate(prefix, oldRoleMapValue, newRoleMapValue, ROLES)
        );
      }

      boolean succeeded = connector.compareAndSwap(updates);
      if (succeeded) {
        cachedGroupMappingMaps.put(prefix, new BasicAuthorizerGroupMappingMapBundle(groupMappingMap, newGroupMappingMapValue));
        cachedRoleMaps.put(prefix, new BasicAuthorizerRoleMapBundle(roleMap, newRoleMapValue));

        byte[] serializedGroupMappingAndRoleMap = getCurrentGroupMappingAndRoleMapSerialized(prefix);
        cacheNotifier.addUpdateGroupMapping(prefix, serializedGroupMappingAndRoleMap);

        return true;
      } else {
        return false;
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  private MetadataCASUpdate createMetadataCASUpdate(
      String prefix,
      byte[] oldValue,
      byte[] newValue,
      String columnName
  )
  {
    return new MetadataCASUpdate(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(prefix, columnName),
        oldValue,
        newValue
    );
  }

  @Override
  public void createUser(String prefix, String userName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    createUserInternal(prefix, userName);
  }

  @Override
  public void deleteUser(String prefix, String userName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    deleteUserInternal(prefix, userName);
  }

  @Override
  public void createGroupMapping(String prefix, BasicAuthorizerGroupMapping groupMapping)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    createGroupMappingInternal(prefix, groupMapping);

  }

  @Override
  public void deleteGroupMapping(String prefix, String groupMappingName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    deleteGroupMappingInternal(prefix, groupMappingName);
  }

  @Override
  public void createRole(String prefix, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    createRoleInternal(prefix, roleName);
  }

  @Override
  public void deleteRole(String prefix, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    deleteRoleInternal(prefix, roleName);
  }

  @Override
  public void assignUserRole(String prefix, String userName, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    assignUserRoleInternal(prefix, userName, roleName);
  }

  @Override
  public void unassignUserRole(String prefix, String userName, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    unassignUserRoleInternal(prefix, userName, roleName);
  }

  @Override
  public void assignGroupMappingRole(String prefix, String groupMappingName, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    assignGroupMappingRoleInternal(prefix, groupMappingName, roleName);
  }

  @Override
  public void unassignGroupMappingRole(String prefix, String groupMappingName, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    unassignGroupMappingRoleInternal(prefix, groupMappingName, roleName);
  }

  @Override
  public void setPermissions(String prefix, String roleName, List<ResourceAction> permissions)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    setPermissionsInternal(prefix, roleName, permissions);
  }

  @Override
  @Nullable
  public Map<String, BasicAuthorizerUser> getCachedUserMap(String prefix)
  {
    BasicAuthorizerUserMapBundle userMapBundle = cachedUserMaps.get(prefix);
    return userMapBundle == null ? null : userMapBundle.getUserMap();
  }

  @Override
  public Map<String, BasicAuthorizerGroupMapping> getCachedGroupMappingMap(String prefix)
  {
    BasicAuthorizerGroupMappingMapBundle groupMapBundle = cachedGroupMappingMaps.get(prefix);
    return groupMapBundle == null ? null : groupMapBundle.getGroupMappingMap();
  }

  @Override
  @Nullable
  public Map<String, BasicAuthorizerRole> getCachedRoleMap(String prefix)
  {
    BasicAuthorizerRoleMapBundle roleMapBundle = cachedRoleMaps.get(prefix);
    return roleMapBundle == null ? null : roleMapBundle.getRoleMap();
  }

  @Override
  public byte[] getCurrentUserMapBytes(String prefix)
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(prefix, USERS)
    );
  }

  @Override
  public byte[] getCurrentGroupMappingMapBytes(String prefix)
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(prefix, GROUP_MAPPINGS)
    );
  }

  @Override
  public byte[] getCurrentRoleMapBytes(String prefix)
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(prefix, ROLES)
    );
  }

  @Override
  public void refreshAllNotification()
  {
    authorizerNames.forEach(
        (authorizerName) -> {
          try {
            byte[] serializedUserAndRoleMap = getCurrentUserAndRoleMapSerialized(authorizerName);
            cacheNotifier.addUpdateUser(authorizerName, serializedUserAndRoleMap);

            byte[] serializeGroupAndRoleMap = getCurrentGroupMappingAndRoleMapSerialized(authorizerName);
            cacheNotifier.addUpdateGroupMapping(authorizerName, serializeGroupAndRoleMap);
          }
          catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
    );
  }

  private byte[] getCurrentUserAndRoleMapSerialized(String prefix) throws IOException
  {
    BasicAuthorizerUserMapBundle userMapBundle = cachedUserMaps.get(prefix);
    BasicAuthorizerRoleMapBundle roleMapBundle = cachedRoleMaps.get(prefix);

    UserAndRoleMap userAndRoleMap = new UserAndRoleMap(
        userMapBundle == null ? null : userMapBundle.getUserMap(),
        roleMapBundle == null ? null : roleMapBundle.getRoleMap()
    );

    return objectMapper.writeValueAsBytes(userAndRoleMap);
  }

  private byte[] getCurrentGroupMappingAndRoleMapSerialized(String prefix) throws IOException
  {
    BasicAuthorizerGroupMappingMapBundle groupMappingMapBundle = cachedGroupMappingMaps.get(prefix);
    BasicAuthorizerRoleMapBundle roleMapBundle = cachedRoleMaps.get(prefix);

    GroupMappingAndRoleMap groupMappingAndRoleMap = new GroupMappingAndRoleMap(
        groupMappingMapBundle == null ? null : groupMappingMapBundle.getGroupMappingMap(),
        roleMapBundle == null ? null : roleMapBundle.getRoleMap()
    );

    return objectMapper.writeValueAsBytes(groupMappingAndRoleMap);
  }

  private void createUserInternal(String prefix, String userName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (createUserOnce(prefix, userName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not create user [%s] due to concurrent update contention.", userName);
  }

  private void deleteUserInternal(String prefix, String userName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (deleteUserOnce(prefix, userName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not delete user [%s] due to concurrent update contention.", userName);
  }

  private void createGroupMappingInternal(String prefix, BasicAuthorizerGroupMapping groupMapping)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (createGroupMappingOnce(prefix, groupMapping)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not create group mapping [%s] due to concurrent update contention.", groupMapping);
  }

  private void deleteGroupMappingInternal(String prefix, String groupMappingName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (deleteGroupMappingOnce(prefix, groupMappingName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not delete group mapping [%s] due to concurrent update contention.", groupMappingName);
  }

  private void createRoleInternal(String prefix, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (createRoleOnce(prefix, roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not create role [%s] due to concurrent update contention.", roleName);
  }

  private void deleteRoleInternal(String prefix, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (deleteRoleOnce(prefix, roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not delete role [%s] due to concurrent update contention.", roleName);
  }

  private void assignUserRoleInternal(String prefix, String userName, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (assignUserRoleOnce(prefix, userName, roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not assign role [%s] to user [%s] due to concurrent update contention.", roleName, userName);
  }

  private void unassignUserRoleInternal(String prefix, String userName, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (unassignUserRoleOnce(prefix, userName, roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not unassign role [%s] from user [%s] due to concurrent update contention.", roleName, userName);
  }

  private void assignGroupMappingRoleInternal(String prefix, String groupMappingName, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (assignGroupMappingRoleOnce(prefix, groupMappingName, roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not assign role [%s] to group mapping [%s] due to concurrent update contention.",
                  roleName,
                  groupMappingName
    );
  }

  private void unassignGroupMappingRoleInternal(String prefix, String groupMappingName, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (unassignGroupMappingRoleOnce(prefix, groupMappingName, roleName)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not unassign role [%s] from group mapping [%s] due to concurrent update contention.", roleName,
                  groupMappingName
    );
  }

  private void setPermissionsInternal(String prefix, String roleName, List<ResourceAction> permissions)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (setPermissionsOnce(prefix, roleName, permissions)) {
        return;
      } else {
        attempts++;
      }
      try {
        Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
      }
      catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
    }
    throw new ISE("Could not set permissions for role [%s] due to concurrent update contention.", roleName);
  }

  private boolean deleteUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(objectMapper, oldValue);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.remove(userName);
    }
    byte[] newValue = BasicAuthUtils.serializeAuthorizerUserMap(objectMapper, userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean createUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(objectMapper, oldValue);
    if (userMap.get(userName) != null) {
      throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
    } else {
      userMap.put(userName, new BasicAuthorizerUser(userName, null));
    }
    byte[] newValue = BasicAuthUtils.serializeAuthorizerUserMap(objectMapper, userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean deleteGroupMappingOnce(String prefix, String groupMappingName)
  {
    byte[] oldValue = getCurrentGroupMappingMapBytes(prefix);
    Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(objectMapper, oldValue);
    if (groupMappingMap.get(groupMappingName) == null) {
      throw new BasicSecurityDBResourceException("Group mapping [%s] does not exist.", groupMappingName);
    } else {
      groupMappingMap.remove(groupMappingName);
    }
    byte[] newValue = BasicAuthUtils.serializeAuthorizerGroupMappingMap(objectMapper, groupMappingMap);
    return tryUpdateGroupMappingMap(prefix, groupMappingMap, oldValue, newValue);
  }

  private boolean createGroupMappingOnce(String prefix, BasicAuthorizerGroupMapping groupMapping)
  {
    byte[] oldValue = getCurrentGroupMappingMapBytes(prefix);
    Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(objectMapper, oldValue);
    if (groupMappingMap.get(groupMapping.getName()) != null) {
      throw new BasicSecurityDBResourceException("Group mapping [%s] already exists.", groupMapping.getName());
    } else {
      groupMappingMap.put(groupMapping.getName(), groupMapping);
    }
    byte[] newValue = BasicAuthUtils.serializeAuthorizerGroupMappingMap(objectMapper, groupMappingMap);
    return tryUpdateGroupMappingMap(prefix, groupMappingMap, oldValue, newValue);
  }

  private boolean createRoleOnce(String prefix, String roleName)
  {
    byte[] oldValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(objectMapper, oldValue);
    if (roleMap.get(roleName) != null) {
      throw new BasicSecurityDBResourceException("Role [%s] already exists.", roleName);
    } else {
      roleMap.put(roleName, new BasicAuthorizerRole(roleName, null));
    }
    byte[] newValue = BasicAuthUtils.serializeAuthorizerRoleMap(objectMapper, roleMap);
    return tryUpdateRoleMap(prefix, roleMap, oldValue, newValue);
  }

  private boolean deleteRoleOnce(String prefix, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    } else {
      roleMap.remove(roleName);
    }

    byte[] oldUserMapValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        oldUserMapValue
    );
    for (BasicAuthorizerUser user : userMap.values()) {
      user.getRoles().remove(roleName);
    }
    byte[] newUserMapValue = BasicAuthUtils.serializeAuthorizerUserMap(objectMapper, userMap);

    byte[] oldGroupMapValue = getCurrentGroupMappingMapBytes(prefix);
    Map<String, BasicAuthorizerGroupMapping> groupMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        oldGroupMapValue
    );
    for (BasicAuthorizerGroupMapping group : groupMap.values()) {
      group.getRoles().remove(roleName);
    }
    byte[] newGroupMapValue = BasicAuthUtils.serializeAuthorizerGroupMappingMap(objectMapper, groupMap);

    byte[] newRoleMapValue = BasicAuthUtils.serializeAuthorizerRoleMap(objectMapper, roleMap);

    return tryUpdateUserAndRoleMap(
        prefix,
        userMap, oldUserMapValue, newUserMapValue,
        roleMap, oldRoleMapValue, newRoleMapValue
    ) && tryUpdateGroupMappingAndRoleMap(
        prefix,
        groupMap, oldGroupMapValue, newGroupMapValue,
        roleMap, newRoleMapValue, newRoleMapValue
    );
  }

  private boolean assignUserRoleOnce(String prefix, String userName, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }

    byte[] oldUserMapValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        oldUserMapValue
    );
    BasicAuthorizerUser user = userMap.get(userName);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    }

    if (user.getRoles().contains(roleName)) {
      throw new BasicSecurityDBResourceException("User [%s] already has role [%s].", userName, roleName);
    }

    user.getRoles().add(roleName);
    byte[] newUserMapValue = BasicAuthUtils.serializeAuthorizerUserMap(objectMapper, userMap);

    // Role map is unchanged, but submit as an update to ensure that the table didn't change (e.g., role deleted)
    return tryUpdateUserAndRoleMap(
        prefix,
        userMap, oldUserMapValue, newUserMapValue,
        roleMap, oldRoleMapValue, oldRoleMapValue
    );
  }

  private boolean unassignUserRoleOnce(String prefix, String userName, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }

    byte[] oldUserMapValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        oldUserMapValue
    );
    BasicAuthorizerUser user = userMap.get(userName);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    }

    if (!user.getRoles().contains(roleName)) {
      throw new BasicSecurityDBResourceException("User [%s] does not have role [%s].", userName, roleName);
    }

    user.getRoles().remove(roleName);
    byte[] newUserMapValue = BasicAuthUtils.serializeAuthorizerUserMap(objectMapper, userMap);

    // Role map is unchanged, but submit as an update to ensure that the table didn't change (e.g., role deleted)
    return tryUpdateUserAndRoleMap(
        prefix,
        userMap, oldUserMapValue, newUserMapValue,
        roleMap, oldRoleMapValue, oldRoleMapValue
    );
  }

  private boolean assignGroupMappingRoleOnce(String prefix, String groupMappingName, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }

    byte[] oldGroupMappingMapValue = getCurrentGroupMappingMapBytes(prefix);
    Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        oldGroupMappingMapValue
    );
    BasicAuthorizerGroupMapping groupMapping = groupMappingMap.get(groupMappingName);
    if (groupMappingMap.get(groupMappingName) == null) {
      throw new BasicSecurityDBResourceException("Group mapping [%s] does not exist.", groupMappingName);
    }

    if (groupMapping.getRoles().contains(roleName)) {
      throw new BasicSecurityDBResourceException("Group mapping [%s] already has role [%s].", groupMappingName, roleName);
    }

    groupMapping.getRoles().add(roleName);
    byte[] newGroupMapValue = BasicAuthUtils.serializeAuthorizerGroupMappingMap(objectMapper, groupMappingMap);

    // Role map is unchanged, but submit as an update to ensure that the table didn't change (e.g., role deleted)
    return tryUpdateGroupMappingAndRoleMap(
        prefix,
        groupMappingMap, oldGroupMappingMapValue, newGroupMapValue,
        roleMap, oldRoleMapValue, oldRoleMapValue
    );
  }

  private boolean unassignGroupMappingRoleOnce(String prefix, String groupMappingName, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }

    byte[] oldGroupMappingMapValue = getCurrentGroupMappingMapBytes(prefix);
    Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        oldGroupMappingMapValue
    );
    BasicAuthorizerGroupMapping groupMapping = groupMappingMap.get(groupMappingName);
    if (groupMappingMap.get(groupMappingName) == null) {
      throw new BasicSecurityDBResourceException("Group mapping [%s] does not exist.", groupMappingName);
    }

    if (!groupMapping.getRoles().contains(roleName)) {
      throw new BasicSecurityDBResourceException("Group mapping [%s] does not have role [%s].", groupMappingName, roleName);
    }

    groupMapping.getRoles().remove(roleName);
    byte[] newGroupMapValue = BasicAuthUtils.serializeAuthorizerGroupMappingMap(objectMapper, groupMappingMap);

    // Role map is unchanged, but submit as an update to ensure that the table didn't change (e.g., role deleted)
    return tryUpdateGroupMappingAndRoleMap(
        prefix,
        groupMappingMap, oldGroupMappingMapValue, newGroupMapValue,
        roleMap, oldRoleMapValue, oldRoleMapValue
    );
  }

  private boolean setPermissionsOnce(String prefix, String roleName, List<ResourceAction> permissions)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        oldRoleMapValue
    );
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }
    roleMap.put(
        roleName,
        new BasicAuthorizerRole(roleName, BasicAuthorizerPermission.makePermissionList(permissions))
    );
    byte[] newRoleMapValue = BasicAuthUtils.serializeAuthorizerRoleMap(objectMapper, roleMap);

    return tryUpdateRoleMap(prefix, roleMap, oldRoleMapValue, newRoleMapValue);
  }

  private void initSuperUsersAndGroupMapping(
      String authorizerName,
      Map<String, BasicAuthorizerUser> userMap,
      Map<String, BasicAuthorizerRole> roleMap,
      Map<String, BasicAuthorizerGroupMapping> groupMappingMap,
      String initialAdminUser,
      String initialAdminRole,
      String initialAdminGroupMapping
  )
  {
    if (!roleMap.containsKey(BasicAuthUtils.ADMIN_NAME)) {
      createRoleInternal(authorizerName, BasicAuthUtils.ADMIN_NAME);
      setPermissionsInternal(authorizerName, BasicAuthUtils.ADMIN_NAME, SUPERUSER_PERMISSIONS);
    }

    if (!roleMap.containsKey(BasicAuthUtils.INTERNAL_USER_NAME)) {
      createRoleInternal(authorizerName, BasicAuthUtils.INTERNAL_USER_NAME);
      setPermissionsInternal(authorizerName, BasicAuthUtils.INTERNAL_USER_NAME, SUPERUSER_PERMISSIONS);
    }

    if (!userMap.containsKey(BasicAuthUtils.ADMIN_NAME)) {
      createUserInternal(authorizerName, BasicAuthUtils.ADMIN_NAME);
      assignUserRoleInternal(authorizerName, BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.ADMIN_NAME);
    }

    if (!userMap.containsKey(BasicAuthUtils.INTERNAL_USER_NAME)) {
      createUserInternal(authorizerName, BasicAuthUtils.INTERNAL_USER_NAME);
      assignUserRoleInternal(authorizerName, BasicAuthUtils.INTERNAL_USER_NAME, BasicAuthUtils.INTERNAL_USER_NAME);
    }

    if (initialAdminRole != null
        && !(initialAdminRole.equals(BasicAuthUtils.ADMIN_NAME) || initialAdminRole.equals(BasicAuthUtils.INTERNAL_USER_NAME))
        && !roleMap.containsKey(initialAdminRole)) {
      createRoleInternal(authorizerName, initialAdminRole);
      setPermissionsInternal(authorizerName, initialAdminRole, SUPERUSER_PERMISSIONS);
    }

    if (initialAdminUser != null
        && !(initialAdminUser.equals(BasicAuthUtils.ADMIN_NAME) || initialAdminUser.equals(BasicAuthUtils.INTERNAL_USER_NAME))
        && !userMap.containsKey(initialAdminUser)) {
      createUserInternal(authorizerName, initialAdminUser);
      assignUserRoleInternal(authorizerName, initialAdminUser, initialAdminRole == null ? BasicAuthUtils.ADMIN_NAME : initialAdminRole);
    }

    if (initialAdminGroupMapping != null && !groupMappingMap.containsKey(BasicAuthUtils.ADMIN_GROUP_MAPPING_NAME)) {
      BasicAuthorizerGroupMapping groupMapping =
          new BasicAuthorizerGroupMapping(
              BasicAuthUtils.ADMIN_GROUP_MAPPING_NAME,
              initialAdminGroupMapping,
              new HashSet<>(Collections.singletonList(initialAdminRole == null ? BasicAuthUtils.ADMIN_NAME : initialAdminRole))
          );
      createGroupMappingInternal(authorizerName, groupMapping);
    }
  }

  private static List<ResourceAction> makeSuperUserPermissions()
  {
    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    return Lists.newArrayList(datasourceR, datasourceW, configR, configW, stateR, stateW);
  }
}
