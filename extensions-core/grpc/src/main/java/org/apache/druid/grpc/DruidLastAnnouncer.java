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

package org.apache.druid.grpc;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.server.DruidNode;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * DiscoverySideEffectsProvider.Child is not public, so we bypass the helper functions and just announce ourselves last.
 * Ideally we should be able to declare side effects in modules in upstream... but cannot at this point.
 * And the DruidBinders.discoveryAnnouncementBinder does not bind to the Last scope, so it announces too quickly
 */
public class DruidLastAnnouncer
{
  private final ServiceAnnouncer announcer;
  private final Set<DruidNode> toAnnounce;
  // Only for testing
  private boolean started = false;

  @Inject
  public DruidLastAnnouncer(
      ServiceAnnouncer announcer,
      Set<DruidNode> toAnnounce
  )
  {
    this.announcer = announcer;
    this.toAnnounce = Collections.unmodifiableSet(new HashSet<>(toAnnounce));
  }

  @LifecycleStart
  public synchronized void start()
  {
    toAnnounce.forEach(announcer::announce);
    started = true;
  }

  @LifecycleStop
  public synchronized void stop()
  {
    // This catches and alerts internally and tries not to throw errors. So we don't worry about catching errors here
    toAnnounce.forEach(announcer::unannounce);
    started = false;
  }

  @VisibleForTesting
  synchronized boolean isStarted()
  {
    return started;
  }
}
