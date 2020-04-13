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
import org.apache.druid.concurrent.LifecycleLock;
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
  private static final EmittingLogger LOG = new EmittingLogger(CliIndexerDataSegmentServerAnnouncerLifecycleHandler.class);

  private final DataSegmentServerAnnouncer dataSegmentServerAnnouncer;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

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
    if (!lifecycleLock.canStart()) {
      throw new RuntimeException("Lifecycle lock could not start");
    }

    try {
      if (lifecycleLock.isStarted()) {
        return;
      }

      LOG.info("Starting...");
      try {
        dataSegmentServerAnnouncer.announce();
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }
      LOG.info("Started.");
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
      throw new RuntimeException("Lifecycle lock could not stop");
    }

    if (!lifecycleLock.isStarted()) {
      return;
    }

    LOG.info("Stopping...");
    try {
      dataSegmentServerAnnouncer.unannounce();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    LOG.info("Stopped.");
  }
}
