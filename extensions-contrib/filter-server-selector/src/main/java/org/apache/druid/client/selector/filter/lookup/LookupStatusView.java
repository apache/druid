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

package org.apache.druid.client.selector.filter.lookup;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.guice.ManageLifecycleAnnouncements;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.lookup.cache.LookupExtractorFactoryMapContainer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycleAnnouncements
public class LookupStatusView
{
  private final LookupsCoordinatorClient client;
  private final ScheduledExecutorService scheduledExec;
  private final LookupStatusViewConfig config;
  private volatile Map<String, Set<String>> hostToLookups = new HashMap<String, Set<String>>();
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  @Inject
  public LookupStatusView(LookupsCoordinatorClient client, LookupStatusViewConfig config)
  {
    this.client = client;
    this.config = config;
    this.scheduledExec = Execs.scheduledSingleThreaded("BrokerServerView-Lookups--%d");
  }

  public boolean hasAllLookups(String server, Set<String> lookups)
  {
    lifecycleLock.awaitStarted();
    Set<String> serverLookups = hostToLookups.get(server);
    return serverLookups != null && serverLookups.containsAll(lookups);
  }

  @LifecycleStart
  public void start() throws InterruptedException
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    
    try {
      CountDownLatch setupLatch = new CountDownLatch(1);
      scheduledExec.scheduleAtFixedRate(new Runnable()
      {
        @Override
        public void run()
        {
          Map<String, Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> lookups = client
              .fetchLookupNodeStatus();
          Map<String, Set<String>> loadedHostToLookups = new HashMap<String, Set<String>>();
          lookups.values().forEach(lookupTier -> {
            lookupTier.forEach((host, lookupState) -> {
              loadedHostToLookups.computeIfAbsent(host.toString(), k -> new HashSet<String>())
                  .addAll(lookupState.getCurrent().keySet());
            });
          });
          hostToLookups = loadedHostToLookups;
          setupLatch.countDown();
        }
      }, 0, config.getPollFrequencySeconds(), TimeUnit.SECONDS);
      
      setupLatch.await();
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
    
    if (scheduledExec != null) {
      scheduledExec.shutdown();
    }
  }
}
