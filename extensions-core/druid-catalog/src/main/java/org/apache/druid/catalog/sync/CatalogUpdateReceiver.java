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

package org.apache.druid.catalog.sync;

import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.joda.time.Duration;

import jakarta.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Receiver which runs in the Broker to listen for catalog updates from the
 * Coordinator. To prevent slowing initial queries, this class loads the
 * current catalog contents into the local cache on lifecycle start, which
 * avoids the on-demand reads that would otherwise occur. After the first load,
 * events from the Coordinator keep the local cache evergreen.
 */
@ManageLifecycle
public class CatalogUpdateReceiver
{
  private static final EmittingLogger LOG = new EmittingLogger(CatalogUpdateReceiver.class);

  private final CachedMetadataCatalog cachedCatalog;
  private final CatalogClientConfig config;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final ScheduledExecutorService exec;

  @Inject
  public CatalogUpdateReceiver(
      final CachedMetadataCatalog cachedCatalog,
      final CatalogClientConfig clientConfig
  )
  {
    this.cachedCatalog = cachedCatalog;
    this.config = clientConfig;
    this.exec = Execs.scheduledSingleThreaded("CatalogUpdateReceiver-Exec--%d");
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      final Duration delay = new Duration(config.getPollingPeriod());
      final long maxDelayFuzz = config.getMaxRandomDelay();
      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          delay,
          delay,
          () -> {
            try {
              long randomDelay = ThreadLocalRandom.current().nextLong(0, maxDelayFuzz);
              Thread.sleep(randomDelay);
              LOG.debug("Scheduled catalog refresh running");
              resync();
              LOG.debug("Scheduled catalog refresh is done");
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while refreshing catalog.").emit();
            }
          }
      );

      lifecycleLock.started();
      LOG.info("Catalog update receiver started");
    }
    finally {
      lifecycleLock.exitStart();
    }

    try {
      resync();
    }
    catch (Throwable t) {
      LOG.warn(t, "Failed to perform initial catalog synchronization");
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    LOG.info("Catalog update receiver stopped");
    exec.shutdownNow();
    lifecycleLock.exitStop();
  }

  private void resync() throws Exception
  {
    RetryUtils.retry(
        () -> {
          cachedCatalog.resync();
          return true;
        },
        e -> true,
        config.getMaxSyncRetries()
    );
  }
}
