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

package org.apache.druid.security.basic.authentication.db.updater;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
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
import org.apache.druid.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.druid.security.basic.authentication.db.cache.BasicAuthenticatorCacheNotifier;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUserMapBundle;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.joda.time.Duration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@ManageLifecycle
public class CoordinatorBasicAuthenticatorMetadataStorageUpdater implements BasicAuthenticatorMetadataStorageUpdater
{
  private static final EmittingLogger LOG =
      new EmittingLogger(CoordinatorBasicAuthenticatorMetadataStorageUpdater.class);
  private static final String USERS = "users";
  private static final long UPDATE_RETRY_DELAY = 1000;

  private final AuthenticatorMapper authenticatorMapper;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ObjectMapper objectMapper;
  private final BasicAuthenticatorCacheNotifier cacheNotifier;
  private final int numRetries = 5;

  private final Map<String, BasicAuthenticatorUserMapBundle> cachedUserMaps;
  private final Set<String> authenticatorPrefixes;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ScheduledExecutorService exec;
  private volatile boolean stopped = false;

  @Inject
  public CoordinatorBasicAuthenticatorMetadataStorageUpdater(
      AuthenticatorMapper authenticatorMapper,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      BasicAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      BasicAuthenticatorCacheNotifier cacheNotifier,
      ConfigManager configManager // -V6022 (unused parameter): ConfigManager creates the db table we need,
                                  // set a dependency here
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorBasicAuthenticatorMetadataStorageUpdater-Exec--%d");
    this.authenticatorMapper = authenticatorMapper;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cacheNotifier = cacheNotifier;
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.authenticatorPrefixes = new HashSet<>();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    if (authenticatorMapper == null || authenticatorMapper.getAuthenticatorMap() == null) {
      return;
    }

    try {
      LOG.info("Starting CoordinatorBasicAuthenticatorMetadataStorageUpdater.");
      for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
        Authenticator authenticator = entry.getValue();
        if (authenticator instanceof BasicHTTPAuthenticator) {
          String authenticatorName = entry.getKey();
          authenticatorPrefixes.add(authenticatorName);
          BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
          BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
          byte[] userMapBytes = getCurrentUserMapBytes(authenticatorName);
          Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
              objectMapper,
              userMapBytes
          );
          cachedUserMaps.put(authenticatorName, new BasicAuthenticatorUserMapBundle(userMap, userMapBytes));

          if (dbConfig.getInitialAdminPassword() != null && !userMap.containsKey(BasicAuthUtils.ADMIN_NAME)) {
            createUserInternal(authenticatorName, BasicAuthUtils.ADMIN_NAME);
            setUserCredentialsInternal(
                authenticatorName,
                BasicAuthUtils.ADMIN_NAME,
                new BasicAuthenticatorCredentialUpdate(
                    dbConfig.getInitialAdminPassword().getPassword(),
                    BasicAuthUtils.DEFAULT_KEY_ITERATIONS
                )
            );
          }

          if (dbConfig.getInitialInternalClientPassword() != null
              && !userMap.containsKey(BasicAuthUtils.INTERNAL_USER_NAME)) {
            createUserInternal(authenticatorName, BasicAuthUtils.INTERNAL_USER_NAME);
            setUserCredentialsInternal(
                authenticatorName,
                BasicAuthUtils.INTERNAL_USER_NAME,
                new BasicAuthenticatorCredentialUpdate(
                    dbConfig.getInitialInternalClientPassword().getPassword(),
                    BasicAuthUtils.DEFAULT_KEY_ITERATIONS
                )
            );
          }
        }
      }

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              if (stopped) {
                return ScheduledExecutors.Signal.STOP;
              }
              try {
                LOG.debug("Scheduled db userMap poll is running");
                for (String authenticatorPrefix : authenticatorPrefixes) {

                  byte[] userMapBytes = getCurrentUserMapBytes(authenticatorPrefix);
                  Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
                      objectMapper,
                      userMapBytes
                  );
                  if (userMapBytes != null) {
                    cachedUserMaps.put(authenticatorPrefix, new BasicAuthenticatorUserMapBundle(userMap, userMapBytes));
                  }
                }
                LOG.debug("Scheduled db userMap poll is done");
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

    LOG.info("CoordinatorBasicAuthenticatorMetadataStorageUpdater is stopping.");
    stopped = true;
    LOG.info("CoordinatorBasicAuthenticatorMetadataStorageUpdater is stopped.");
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
  public void setUserCredentials(String prefix, String userName, BasicAuthenticatorCredentialUpdate update)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    setUserCredentialsInternal(prefix, userName, update);
  }

  @Override
  public Map<String, BasicAuthenticatorUser> getCachedUserMap(String prefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    BasicAuthenticatorUserMapBundle bundle = cachedUserMaps.get(prefix);
    if (bundle == null) {
      return null;
    } else {
      return bundle.getUserMap();
    }
  }

  @Override
  public byte[] getCachedSerializedUserMap(String prefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    BasicAuthenticatorUserMapBundle bundle = cachedUserMaps.get(prefix);
    if (bundle == null) {
      return null;
    } else {
      return bundle.getSerializedUserMap();
    }
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
  public void refreshAllNotification()
  {
    cachedUserMaps.forEach(
        (authenticatorName, userMapBundle) -> {
          cacheNotifier.addUserUpdate(authenticatorName, userMapBundle.getSerializedUserMap());
        }
    );
  }

  private static String getPrefixedKeyColumn(String keyPrefix, String keyName)
  {
    return StringUtils.format("basic_authentication_%s_%s", keyPrefix, keyName);
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
      updateRetryDelay();
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
      updateRetryDelay();
    }
    throw new ISE("Could not delete user[%s] due to concurrent update contention.", userName);
  }

  private void updateRetryDelay()
  {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  private void setUserCredentialsInternal(String prefix, String userName, BasicAuthenticatorCredentialUpdate update)
  {
    BasicAuthenticatorCredentials credentials;

    // use default iteration count from Authenticator if not specified in request
    if (update.getIterations() == -1) {
      BasicHTTPAuthenticator authenticator = (BasicHTTPAuthenticator) authenticatorMapper.getAuthenticatorMap().get(
          prefix
      );
      credentials = new BasicAuthenticatorCredentials(
          new BasicAuthenticatorCredentialUpdate(
              update.getPassword(),
              authenticator.getDbConfig().getCredentialIterations()
          )
      );
    } else {
      credentials = new BasicAuthenticatorCredentials(update);
    }

    int attempts = 0;
    while (attempts < numRetries) {
      if (setUserCredentialOnce(prefix, userName, credentials)) {
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
    throw new ISE("Could not set credentials for user[%s] due to concurrent update contention.", userName);
  }

  private boolean createUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        oldValue
    );
    if (userMap.get(userName) != null) {
      throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
    } else {
      userMap.put(userName, new BasicAuthenticatorUser(userName, null));
    }
    byte[] newValue = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean deleteUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        oldValue
    );
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.remove(userName);
    }
    byte[] newValue = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean setUserCredentialOnce(String prefix, String userName, BasicAuthenticatorCredentials credentials)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        oldValue
    );
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.put(userName, new BasicAuthenticatorUser(userName, credentials));
    }
    byte[] newValue = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean tryUpdateUserMap(
      String prefix,
      Map<String, BasicAuthenticatorUser> userMap,
      byte[] oldValue,
      byte[] newValue
  )
  {
    try {
      MetadataCASUpdate update = new MetadataCASUpdate(
          connectorConfig.getConfigTable(),
          MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
          MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
          getPrefixedKeyColumn(prefix, USERS),
          oldValue,
          newValue
      );

      boolean succeeded = connector.compareAndSwap(
          Collections.singletonList(update)
      );

      if (succeeded) {
        cachedUserMaps.put(prefix, new BasicAuthenticatorUserMapBundle(userMap, newValue));
        cacheNotifier.addUserUpdate(prefix, newValue);
        return true;
      } else {
        return false;
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
