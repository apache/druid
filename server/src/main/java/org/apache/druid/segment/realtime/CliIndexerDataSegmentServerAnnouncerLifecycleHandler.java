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

package org.apache.druid.segment.realtime;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;

import java.io.IOException;

/**
 * Ties the {@link DataSegmentServerAnnouncer} announce/unannounce to the lifecycle start and stop.
 *
 * Analogous to {@link org.apache.druid.server.coordination.SegmentLoadDropHandler} on the Historicals,
 * but without segment cache management.
 */
@ManageLifecycle
public class CliIndexerDataSegmentServerAnnouncerLifecycleHandler
{
  private static final EmittingLogger log = new EmittingLogger(CliIndexerDataSegmentServerAnnouncerLifecycleHandler.class);

  private final DataSegmentServerAnnouncer dataSegmentServerAnnouncer;

  // Synchronizes start/stop of this object.
  private final Object startStopLock = new Object();

  private volatile boolean started = false;

  @Inject
  public CliIndexerDataSegmentServerAnnouncerLifecycleHandler(
      DataSegmentServerAnnouncer dataSegmentServerAnnouncer
  )
  {
    this.dataSegmentServerAnnouncer = dataSegmentServerAnnouncer;
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (startStopLock) {
      if (started) {
        return;
      }

      log.info("Starting...");
      try {
        dataSegmentServerAnnouncer.announce();
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      started = true;
      log.info("Started.");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (!started) {
        return;
      }

      log.info("Stopping...");
      try {
        dataSegmentServerAnnouncer.unannounce();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
      finally {
        started = false;
      }
      log.info("Stopped.");
    }
  }

  public boolean isStarted()
  {
    return started;
  }
}
