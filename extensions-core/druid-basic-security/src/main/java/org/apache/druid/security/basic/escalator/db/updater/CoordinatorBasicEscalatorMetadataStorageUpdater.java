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

package org.apache.druid.security.basic.escalator.db.updater;

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
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.escalator.db.cache.BasicEscalatorCacheNotifier;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredentialBundle;
import org.joda.time.Duration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@ManageLifecycle
public class CoordinatorBasicEscalatorMetadataStorageUpdater implements BasicEscalatorMetadataStorageUpdater
{
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorBasicEscalatorMetadataStorageUpdater.class);


  private static final String CREDENTIAL = "credential";
  private static final long UPDATE_RETRY_DELAY = 1000;

  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final BasicAuthCommonCacheConfig commonCacheConfig;
  private final ObjectMapper objectMapper;
  private final BasicEscalatorCacheNotifier cacheNotifier;
  private final int numRetries = 5;

  private final AtomicReference<BasicEscalatorCredentialBundle> cachedEscalatorCredential;
  private final Set<String> authenticatorPrefixes;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ScheduledExecutorService exec;
  private volatile boolean stopped = false;

  @Inject
  public CoordinatorBasicEscalatorMetadataStorageUpdater(
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      BasicAuthCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      BasicEscalatorCacheNotifier cacheNotifier,
      ConfigManager configManager // ConfigManager creates the db table we need, set a dependency here
  )
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorBasicEscalatorMetadataStorageUpdater-Exec--%d");
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cacheNotifier = cacheNotifier;
    this.cachedEscalatorCredential = new AtomicReference<>();
    this.authenticatorPrefixes = new HashSet<>();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      LOG.info("Starting CoordinatorBasicEscalatorMetadataStorageUpdater.");
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
                LOG.debug("Scheduled db escalator credential poll is running");
                byte[] escalatorCredentialBytes = getCurrentEscalatorCredentialBytes();

                if (escalatorCredentialBytes != null) {
                  BasicEscalatorCredential escalatorCredential = BasicAuthUtils.deserializeEscalatorCredential(
                      objectMapper,
                      escalatorCredentialBytes
                  );
                  if (escalatorCredential != null) {
                    cachedEscalatorCredential.set(new BasicEscalatorCredentialBundle(escalatorCredential, escalatorCredentialBytes));
                  }
                }
                LOG.debug("Scheduled db escalator credential poll is done");
              }
              catch (Throwable t) {
                LOG.makeAlert(t, "Error occured while polling for cachedEscalatorCredential.").emit();
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

    LOG.info("CoordinatorBasicEscalatorMetadataStorageUpdater is stopping.");
    stopped = true;
    LOG.info("CoordinatorBasicEscalatorMetadataStorageUpdater is stopped.");
  }



  @Override
  public void refreshAllNotification()
  {
    cacheNotifier.addEscalatorCredentialUpdate(cachedEscalatorCredential.get().getSerializedEscalatorCredential());
  }

  @Override
  public void updateEscalatorCredential(BasicEscalatorCredential escalatorCredential)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    updateEscalatorCredentialInternal(escalatorCredential);
  }

  @Override
  public BasicEscalatorCredential getCachedEscalatorCredential()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    BasicEscalatorCredentialBundle bundle = cachedEscalatorCredential.get();
    if (bundle == null) {
      return null;
    } else {
      return bundle.getEscalatorCredential();
    }
  }

  @Override
  public byte[] getCachedSerializedEscalatorCredential()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    BasicEscalatorCredentialBundle bundle = cachedEscalatorCredential.get();
    if (bundle == null) {
      return null;
    } else {
      return bundle.getSerializedEscalatorCredential();
    }
  }

  @Override
  public byte[] getCurrentEscalatorCredentialBytes()
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getKeyColumn(CREDENTIAL)
    );
  }

  private static String getKeyColumn(String keyName)
  {
    return StringUtils.format("basic_escalator_%s", keyName);
  }

  private void updateEscalatorCredentialInternal(BasicEscalatorCredential credential)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (updateEscalatorCredentialOnce(credential)) {
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
    throw new ISE("Could not update escalator credential due to concurrent update contention.");
  }

  private boolean updateEscalatorCredentialOnce(BasicEscalatorCredential credential)
  {
    byte[] oldValue = getCurrentEscalatorCredentialBytes();
    byte[] newValue = BasicAuthUtils.serializeEscalatorCredential(objectMapper, credential);
    return tryUpdateEscalatorCredential(credential, oldValue, newValue);
  }

  private boolean tryUpdateEscalatorCredential(
      BasicEscalatorCredential credential,
      byte[] oldValue,
      byte[] newValue
  )
  {
    try {
      MetadataCASUpdate update = new MetadataCASUpdate(
          connectorConfig.getConfigTable(),
          MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
          MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
          getKeyColumn(CREDENTIAL),
          oldValue,
          newValue
      );

      boolean succeeded = connector.compareAndSwap(
          Collections.singletonList(update)
      );

      if (succeeded) {
        cachedEscalatorCredential.set(new BasicEscalatorCredentialBundle(credential, newValue));
        cacheNotifier.addEscalatorCredentialUpdate(newValue);
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
