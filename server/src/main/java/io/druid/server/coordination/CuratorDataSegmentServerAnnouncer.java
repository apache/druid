/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.curator.announcement.Announcer;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;

/**
 */
public class CuratorDataSegmentServerAnnouncer implements DataSegmentServerAnnouncer
{
  private static final Logger log = new Logger(CuratorDataSegmentServerAnnouncer.class);

  private final DruidServerMetadata server;
  private final ZkPathsConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;

  private final Object lock = new Object();

  private volatile boolean announced = false;

  @Inject
  public CuratorDataSegmentServerAnnouncer(
      DruidServerMetadata server,
      ZkPathsConfig config,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    this.server = server;
    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void announce()
  {
    synchronized (lock) {
      if (announced) {
        return;
      }

      try {
        final String path = makeAnnouncementPath();
        log.info("Announcing self[%s] at [%s]", server, path);
        announcer.announce(path, jsonMapper.writeValueAsBytes(server), false);
      }
      catch (JsonProcessingException e) {
        throw Throwables.propagate(e);
      }

      announced = true;
    }
  }

  @Override
  public void unannounce()
  {
    synchronized (lock) {
      if (!announced) {
        return;
      }

      final String path = makeAnnouncementPath();
      log.info("Unannouncing self[%s] at [%s]", server, path);
      announcer.unannounce(path);

      announced = false;
    }
  }

  private String makeAnnouncementPath()
  {
    return ZKPaths.makePath(config.getAnnouncementsPath(), server.getName());
  }
}
