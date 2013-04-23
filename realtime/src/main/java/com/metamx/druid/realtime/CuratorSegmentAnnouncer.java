/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.realtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.curator.announcement.Announcer;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class CuratorSegmentAnnouncer implements SegmentAnnouncer
{
  private static final Logger log = new Logger(CuratorSegmentAnnouncer.class);

  private final Object lock = new Object();

  private final ZkSegmentAnnouncerConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final String servedSegmentsLocation;

  private volatile boolean started = false;

  public CuratorSegmentAnnouncer(
      ZkSegmentAnnouncerConfig config,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
    this.servedSegmentsLocation = ZKPaths.makePath(config.getServedSegmentsPath(), config.getServerName());
  }

  private Map<String, String> getStringProps()
  {
    return ImmutableMap.of(
        "name", config.getServerName(),
        "host", config.getHost(),
        "maxSize", String.valueOf(config.getMaxSize()),
        "type", config.getServerType()
    );
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Starting CuratorSegmentAnnouncer for server[%s] with config[%s]", config.getServerName(), config);
      try {
        announcer.announce(makeAnnouncementPath(), jsonMapper.writeValueAsBytes(getStringProps()));
      }
      catch (JsonProcessingException e) {
        throw Throwables.propagate(e);
      }

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      log.info("Stopping CuratorSegmentAnnouncer with config[%s]", config);
      announcer.unannounce(makeAnnouncementPath());

      started = false;
    }
  }

  public void announceSegment(DataSegment segment) throws IOException
  {
    log.info("Announcing realtime segment %s", segment.getIdentifier());
    announcer.announce(makeServedSegmentPath(segment), jsonMapper.writeValueAsBytes(segment));
  }

  public void unannounceSegment(DataSegment segment) throws IOException
  {
    log.info("Unannouncing realtime segment %s", segment.getIdentifier());
    announcer.unannounce(makeServedSegmentPath(segment));
  }

  private String makeAnnouncementPath() {
    return ZKPaths.makePath(config.getAnnouncementsPath(), config.getServerName());
  }

  private String makeServedSegmentPath(DataSegment segment)
  {
    return ZKPaths.makePath(servedSegmentsLocation, segment.getIdentifier());
  }
}
