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

package io.druid.security.basic.authorization.db.updater;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.config.ConfigManager;
import io.druid.concurrent.LifecycleLock;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.metadata.MetadataCASUpdate;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.BasicAuthCommonCacheConfig;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheNotifier;
import io.druid.security.basic.authorization.entity.BasicAuthorizerPermission;
import io.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import io.druid.security.basic.authorization.entity.UserAndRoleMap;
import io.druid.server.security.Action;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthorizerMetadataStorageUpdater implements BasicAuthorizerMetadataStorageUpdater
{
  private static final EmittingLogger LOG =
      new EmittingLogger(CoordinatorBasicAuthorizerMetadataStorageUpdater.class);

  public static final List<ResourceAction> SUPERUSER_PERMISSIONS = makeSuperUserPermissions();

  public static final String USERS = "users";
  public static final String ROLES = "roles";
  public static final TypeReference USER_MAP_TYPE_REFERENCE = new TypeReference<Map<String, BasicAuthorizerUser>>()
  {
  };
  public static final TypeReference ROLE_MAP_TYPE_REFERENCE = new TypeReference<Map<String, BasicAuthorizerRole>>()
  {
  };
  public static final TypeReference USER_AND_ROLE_MAP_TYPE_REFERENCE = new TypeReference<UserAndRoleMap>()
  {
  };

  public static final String DEFAULT_ADMIN_NAME = "admin";
  public static final String DEFAULT_INTERNAL_SYSTEM_NAME = "druid_system";
  
  private final AuthorizerMapper authorizerMapper;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final BasicAuthorizerCacheNotifier cacheNotifier;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ObjectMapper objectMapper;
  private final int numRetries = 5;

  private final Map<String, Map<String, BasicAuthorizerUser>> cachedUserMaps;
  private final Map<String, byte[]> cachedSerializedUserMaps;

  private final Map<String, Map<String, BasicAuthorizerRole>> cachedRoleMaps;
  private final Map<String, byte[]> cachedSerializedRoleMaps;

  private final Set<String> authorizerNames;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private volatile ScheduledExecutorService exec;
  private volatile boolean stopped = false;

  @Inject
  public CoordinatorBasicAuthorizerMetadataStorageUpdater(
      AuthorizerMapper authorizerMapper,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      BasicAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      BasicAuthorizerCacheNotifier cacheNotifier,
      ConfigManager configManager // ConfigManager creates the db table we need, set a dependency here
  )
  {
    this.authorizerMapper = authorizerMapper;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cacheNotifier = cacheNotifier;
    this.cachedUserMaps = new HashMap<>();
    this.cachedSerializedUserMaps = new HashMap<>();
    this.cachedRoleMaps = new HashMap<>();
    this.cachedSerializedRoleMaps = new HashMap<>();
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
          String authorizerName = entry.getKey();
          authorizerNames.add(authorizerName);
          BasicRoleBasedAuthorizer basicRoleBasedAuthorizer = (BasicRoleBasedAuthorizer) authorizer;

          byte[] userMapBytes = getCurrentUserMapBytes(authorizerName);
          Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(userMapBytes);
          cachedUserMaps.put(authorizerName, userMap);
          cachedSerializedUserMaps.put(authorizerName, userMapBytes);

          byte[] roleMapBytes = getCurrentRoleMapBytes(authorizerName);
          Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(roleMapBytes);
          cachedRoleMaps.put(authorizerName, roleMap);
          cachedSerializedRoleMaps.put(authorizerName, roleMapBytes);

          initSuperusers(authorizerName, userMap, roleMap);
        }
      }

      this.exec = Execs.scheduledSingleThreaded("CoordinatorBasicAuthorizerMetadataStorageUpdater-Exec--%d");

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call() throws Exception
            {
              if (stopped) {
                return ScheduledExecutors.Signal.STOP;
              }
              try {
                LOG.debug("Scheduled db poll is running");
                for (String authorizerName : authorizerNames) {

                  byte[] userMapBytes = getCurrentUserMapBytes(authorizerName);
                  Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(userMapBytes);
                  if (userMapBytes != null) {
                    synchronized (cachedUserMaps) {
                      cachedUserMaps.put(authorizerName, userMap);
                      cachedSerializedUserMaps.put(authorizerName, userMapBytes);
                    }
                  }

                  byte[] roleMapBytes = getCurrentRoleMapBytes(authorizerName);
                  Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(roleMapBytes);
                  if (roleMapBytes != null) {
                    synchronized (cachedUserMaps) {
                      cachedRoleMaps.put(authorizerName, roleMap);
                      cachedSerializedRoleMaps.put(authorizerName, roleMapBytes);
                    }
                  }
                }
                LOG.debug("Scheduled db poll is done");
              }
              catch (Throwable t) {
                LOG.makeAlert(t, "Error occured while polling for cachedUserMaps.").emit();
              }
              return ScheduledExecutors.Signal.REPEAT;
            }
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
    return tryUpdateUserAndRoleMap(prefix, userMap, oldUserMapValue, newUserMapValue, null, null, null);
  }

  private boolean tryUpdateRoleMap(
      String prefix,
      Map<String, BasicAuthorizerRole> roleMap,
      byte[] oldRoleMapValue,
      byte[] newRoleMapValue
  )
  {
    return tryUpdateUserAndRoleMap(prefix, null, null, null, roleMap, oldRoleMapValue, newRoleMapValue);
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
      synchronized (cachedRoleMaps) {
        List<MetadataCASUpdate> updates = new ArrayList<>();
        if (userMap != null) {
          updates.add(
              new MetadataCASUpdate(
                  connectorConfig.getConfigTable(),
                  MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
                  MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
                  getPrefixedKeyColumn(prefix, USERS),
                  oldUserMapValue,
                  newUserMapValue
              )
          );
        }

        if (roleMap != null) {
          updates.add(
              new MetadataCASUpdate(
                  connectorConfig.getConfigTable(),
                  MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
                  MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
                  getPrefixedKeyColumn(prefix, ROLES),
                  oldRoleMapValue,
                  newRoleMapValue
              )
          );
        }

        boolean succeeded = connector.compareAndSwap(updates);
        if (succeeded) {
          if (userMap != null) {
            cachedUserMaps.put(prefix, userMap);
            cachedSerializedUserMaps.put(prefix, newUserMapValue);
          }
          if (roleMap != null) {
            cachedRoleMaps.put(prefix, roleMap);
            cachedSerializedRoleMaps.put(prefix, newRoleMapValue);
          }

          UserAndRoleMap userAndRoleMap = new UserAndRoleMap(
              cachedUserMaps.get(prefix),
              cachedRoleMaps.get(prefix)
          );

          byte[] serializedUserAndRoleMap = objectMapper.writeValueAsBytes(userAndRoleMap);
          cacheNotifier.addUpdate(prefix, serializedUserAndRoleMap);

          return true;
        } else {
          return false;
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
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
  public void assignRole(String prefix, String userName, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    assignRoleInternal(prefix, userName, roleName);
  }

  @Override
  public void unassignRole(String prefix, String userName, String roleName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    unassignRoleInternal(prefix, userName, roleName);
  }

  @Override
  public void setPermissions(
      String prefix, String roleName, List<ResourceAction> permissions
  )
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    setPermissionsInternal(prefix, roleName, permissions);
  }

  @Override
  public Map<String, BasicAuthorizerUser> getCachedUserMap(String prefix)
  {
    return cachedUserMaps.get(prefix);
  }

  @Override
  public Map<String, BasicAuthorizerRole> getCachedRoleMap(String prefix)
  {
    return cachedRoleMaps.get(prefix);
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
  public Map<String, BasicAuthorizerUser> deserializeUserMap(byte[] userMapBytes)
  {
    Map<String, BasicAuthorizerUser> userMap;
    if (userMapBytes == null) {
      userMap = Maps.newHashMap();
    } else {
      try {
        userMap = objectMapper.readValue(userMapBytes, USER_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return userMap;
  }

  public byte[] serializeUserMap(Map<String, BasicAuthorizerUser> userMap)
  {
    try {
      return objectMapper.writeValueAsBytes(userMap);
    }
    catch (IOException ioe) {
      throw new ISE("WTF? Couldn't serialize userMap!");
    }
  }

  @Override
  public Map<String, BasicAuthorizerRole> deserializeRoleMap(byte[] roleMapBytes)
  {
    Map<String, BasicAuthorizerRole> roleMap;
    if (roleMapBytes == null) {
      roleMap = Maps.newHashMap();
    } else {
      try {
        roleMap = objectMapper.readValue(roleMapBytes, ROLE_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return roleMap;
  }

  public byte[] serializeRoleMap(Map<String, BasicAuthorizerRole> roleMap)
  {
    try {
      return objectMapper.writeValueAsBytes(roleMap);
    }
    catch (IOException ioe) {
      throw new ISE("WTF? Couldn't serialize roleMap!");
    }
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
    }
    throw new ISE("Could not create user[%s] due to concurrent update contention.", userName);
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
    }
    throw new ISE("Could not delete user[%s] due to concurrent update contention.", userName);
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
    }
    throw new ISE("Could not create role[%s] due to concurrent update contention.", roleName);
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
    }
    throw new ISE("Could not delete role[%s] due to concurrent update contention.", roleName);
  }

  private void assignRoleInternal(String prefix, String userName, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (assignRoleOnce(prefix, userName, roleName)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not assign role[%s] to user[%s] due to concurrent update contention.", roleName, userName);
  }

  private void unassignRoleInternal(String prefix, String userName, String roleName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (unassignRoleOnce(prefix, userName, roleName)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not unassign role[%s] from user[%s] due to concurrent update contention.", roleName, userName);
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
    }
    throw new ISE("Could not set permissions for role[%s] due to concurrent update contention.", roleName);
  }

  private boolean createUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) != null) {
      throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
    } else {
      userMap.put(userName, new BasicAuthorizerUser(userName, null));
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean deleteUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.remove(userName);
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean createRoleOnce(String prefix, String roleName)
  {
    byte[] oldValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(oldValue);
    if (roleMap.get(roleName) != null) {
      throw new BasicSecurityDBResourceException("Role [%s] already exists.", roleName);
    } else {
      roleMap.put(roleName, new BasicAuthorizerRole(roleName, null));
    }
    byte[] newValue = serializeRoleMap(roleMap);
    return tryUpdateRoleMap(prefix, roleMap, oldValue, newValue);
  }

  private boolean deleteRoleOnce(String prefix, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(oldRoleMapValue);
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    } else {
      roleMap.remove(roleName);
    }

    byte[] oldUserMapValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(oldUserMapValue);
    for (BasicAuthorizerUser user : userMap.values()) {
      user.getRoles().remove(roleName);
    }
    byte[] newUserMapValue = serializeUserMap(userMap);
    byte[] newRoleMapValue = serializeRoleMap(roleMap);

    return tryUpdateUserAndRoleMap(
        prefix,
        userMap, oldUserMapValue, newUserMapValue,
        roleMap, oldRoleMapValue, newRoleMapValue
    );
  }

  private boolean assignRoleOnce(String prefix, String userName, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(oldRoleMapValue);
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }

    byte[] oldUserMapValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(oldUserMapValue);
    BasicAuthorizerUser user = userMap.get(userName);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    }

    if (user.getRoles().contains(roleName)) {
      throw new BasicSecurityDBResourceException("User [%s] already has role [%s].", userName, roleName);
    }

    user.getRoles().add(roleName);
    byte[] newUserMapValue = serializeUserMap(userMap);

    // Role map is unchanged, but submit as an update to ensure that the table didn't change (e.g., role deleted)
    return tryUpdateUserAndRoleMap(
        prefix,
        userMap, oldUserMapValue, newUserMapValue,
        roleMap, oldRoleMapValue, oldRoleMapValue
    );
  }

  private boolean unassignRoleOnce(String prefix, String userName, String roleName)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(oldRoleMapValue);
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }

    byte[] oldUserMapValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthorizerUser> userMap = deserializeUserMap(oldUserMapValue);
    BasicAuthorizerUser user = userMap.get(userName);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    }

    if (!user.getRoles().contains(roleName)) {
      throw new BasicSecurityDBResourceException("User [%s] does not have role [%s].", userName, roleName);
    }

    user.getRoles().remove(roleName);
    byte[] newUserMapValue = serializeUserMap(userMap);

    // Role map is unchanged, but submit as an update to ensure that the table didn't change (e.g., role deleted)
    return tryUpdateUserAndRoleMap(
        prefix,
        userMap, oldUserMapValue, newUserMapValue,
        roleMap, oldRoleMapValue, oldRoleMapValue
    );
  }

  private boolean setPermissionsOnce(String prefix, String roleName, List<ResourceAction> permissions)
  {
    byte[] oldRoleMapValue = getCurrentRoleMapBytes(prefix);
    Map<String, BasicAuthorizerRole> roleMap = deserializeRoleMap(oldRoleMapValue);
    if (roleMap.get(roleName) == null) {
      throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
    }
    roleMap.put(
        roleName,
        new BasicAuthorizerRole(roleName, BasicAuthorizerPermission.makePermissionList(permissions))
    );
    byte[] newRoleMapValue = serializeRoleMap(roleMap);

    return tryUpdateRoleMap(prefix, roleMap, oldRoleMapValue, newRoleMapValue);
  }

  private void initSuperusers(
      String authorizerName,
      Map<String, BasicAuthorizerUser> userMap,
      Map<String, BasicAuthorizerRole> roleMap
  )
  {
    if (!roleMap.containsKey(DEFAULT_ADMIN_NAME)) {
      createRoleInternal(authorizerName, DEFAULT_ADMIN_NAME);
      setPermissionsInternal(authorizerName, DEFAULT_ADMIN_NAME, SUPERUSER_PERMISSIONS);
    }

    if (!roleMap.containsKey(DEFAULT_INTERNAL_SYSTEM_NAME)) {
      createRoleInternal(authorizerName, DEFAULT_INTERNAL_SYSTEM_NAME);
      setPermissionsInternal(authorizerName, DEFAULT_INTERNAL_SYSTEM_NAME, SUPERUSER_PERMISSIONS);
    }

    if (!userMap.containsKey(DEFAULT_INTERNAL_SYSTEM_NAME)) {
      createUserInternal(authorizerName, DEFAULT_INTERNAL_SYSTEM_NAME);
      assignRoleInternal(authorizerName, DEFAULT_INTERNAL_SYSTEM_NAME, DEFAULT_INTERNAL_SYSTEM_NAME);
    }

    if (!userMap.containsKey(DEFAULT_ADMIN_NAME)) {
      createUserInternal(authorizerName, DEFAULT_ADMIN_NAME);
      assignRoleInternal(authorizerName, DEFAULT_ADMIN_NAME, DEFAULT_ADMIN_NAME);
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
