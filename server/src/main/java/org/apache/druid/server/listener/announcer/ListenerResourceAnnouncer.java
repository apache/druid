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

package org.apache.druid.server.listener.announcer;

import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.http.HostAndPortWithScheme;

import java.nio.ByteBuffer;

/**
 * Starting 0.11.0 Coordinator uses announcements made by {@link org.apache.druid.discovery.DruidNodeAnnouncer} .
 */
@Deprecated
public abstract class ListenerResourceAnnouncer
{
  private static final byte[] ANNOUNCE_BYTES = ByteBuffer
      .allocate(Long.BYTES)
      .putLong(System.currentTimeMillis())
      .array();
  private static final Logger LOG = new Logger(ListenerResourceAnnouncer.class);
  private final Object startStopSync = new Object();
  private volatile boolean started = false;
  private final Announcer announcer;
  private final String announcePath;

  public ListenerResourceAnnouncer(
      Announcer announcer,
      ListeningAnnouncerConfig listeningAnnouncerConfig,
      String listener_key,
      HostAndPortWithScheme node
  )
  {
    this(
        announcer,
        ZKPaths.makePath(listeningAnnouncerConfig.getListenersPath(), listener_key),
        node
    );
  }

  ListenerResourceAnnouncer(
      Announcer announcer,
      String announceBasePath,
      HostAndPortWithScheme node
  )
  {
    this.announcePath = ZKPaths.makePath(announceBasePath, node.toString());
    this.announcer = announcer;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (startStopSync) {
      if (started) {
        LOG.debug("Already started, ignoring");
        return;
      }
      try {
        // Announcement is based on MS. This is to make sure we don't collide on announcements
        Thread.sleep(2);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      announcer.announce(announcePath, ANNOUNCE_BYTES);
      LOG.info("Announcing start time on [%s]", announcePath);
      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopSync) {
      if (!started) {
        LOG.debug("Already stopped, ignoring");
        return;
      }
      announcer.unannounce(announcePath);
      LOG.info("Unannouncing start time on [%s]", announcePath);
      started = false;
    }
  }

  public byte[] getAnnounceBytes()
  {
    return ByteBuffer.allocate(ANNOUNCE_BYTES.length).put(ANNOUNCE_BYTES).array();
  }
}
