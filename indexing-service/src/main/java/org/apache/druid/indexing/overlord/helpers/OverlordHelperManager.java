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

package org.apache.druid.indexing.overlord.helpers;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class OverlordHelperManager
{
  private static final Logger log = new Logger(OverlordHelperManager.class);

  private final ScheduledExecutorFactory execFactory;
  private final Set<OverlordHelper> helpers;

  private volatile ScheduledExecutorService exec;
  private final Object startStopLock = new Object();
  private volatile boolean started = false;

  @Inject
  public OverlordHelperManager(
      ScheduledExecutorFactory scheduledExecutorFactory,
      Set<OverlordHelper> helpers)
  {
    this.execFactory = scheduledExecutorFactory;
    this.helpers = helpers;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (startStopLock) {
      if (!started) {
        log.info("OverlordHelperManager is starting.");

        for (OverlordHelper helper : helpers) {
          if (helper.isEnabled()) {
            if (exec == null) {
              exec = execFactory.create(1, "Overlord-Helper-Manager-Exec--%d");
            }
            helper.schedule(exec);
          }
        }
        started = true;

        log.info("OverlordHelperManager is started.");
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (started) {
        log.info("OverlordHelperManager is stopping.");
        if (exec != null) {
          exec.shutdownNow();
          exec = null;
        }
        started = false;

        log.info("OverlordHelperManager is stopped.");
      }
    }
  }
}
