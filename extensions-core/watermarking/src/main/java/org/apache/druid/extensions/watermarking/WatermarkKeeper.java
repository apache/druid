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

package org.apache.druid.extensions.watermarking;

import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.extensions.watermarking.storage.WatermarkSource;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;

@ManageLifecycle
public class WatermarkKeeper
{
  public static final String SERVICE_NAME = "watermark-keeper";
  private static final Logger log = new Logger(WatermarkKeeper.class);
  private final DruidNode self;
  private final ServiceAnnouncer serviceAnnouncer;
  private final WatermarkSource watermarkSource;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  @Inject
  public WatermarkKeeper(
      ServiceAnnouncer serviceAnnouncer,
      WatermarkSource source,
      @Self DruidNode node
  )
  {
    this.self = node;
    this.serviceAnnouncer = serviceAnnouncer;
    this.watermarkSource = source;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("LookupCoordinatorManager can't start.");
      }
      log.info("Starting WatermarkKeeper.");
      try {
        watermarkSource.initialize();
        lifecycleLock.started();
        serviceAnnouncer.announce(self);
      }
      finally {
        lifecycleLock.exitStart();
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStop()) {
        throw new ISE("can't stop.");
      }

      log.info("Stopping WatermarkKeeper.");
      serviceAnnouncer.unannounce(self);
    }
  }
}
