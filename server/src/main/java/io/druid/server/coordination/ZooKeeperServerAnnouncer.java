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
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.Capability;
import io.druid.server.initialization.ZkPathsConfig;

import java.util.Set;

/**
 */
public class ZooKeeperServerAnnouncer extends AbstractServerAnnouncer
{

  private static final Logger log = new Logger(ZooKeeperServerAnnouncer.class);

  private final DruidServerMetadata me;
  private final ObjectMapper objectMapper;
  private final ZkPathsConfig zkPathsConfig;
  private final Announcer announcer;
  private final Set<String> capabilities;
  private final String selfAnnouncementPath;
  private final String leadershipPath;

  @Inject
  public ZooKeeperServerAnnouncer(
      DruidServerMetadata me,
      ObjectMapper mapper,
      ZkPathsConfig zkPathsConfig,
      Announcer announcer,
      @Capability Set<String> capabilities
  )
  {
    this.me = me;
    this.objectMapper = mapper;
    this.zkPathsConfig = zkPathsConfig;
    this.announcer = announcer;
    this.capabilities = capabilities;
    this.selfAnnouncementPath = zkPathsConfig.getAnnouncementPathForServer(me);
    this.leadershipPath = zkPathsConfig.getLeadershipPathForServer(me);
  }

  @Override
  public void announceLeadership()
  {
    log.info("Announcing self [%s] as leader at [%s]", me, selfAnnouncementPath);
    try {
      announcer.announce(leadershipPath, objectMapper.writeValueAsBytes(me), false);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void unannounceLeadership()
  {
    log.info("Unannouncing self [%s] as leader at [%s]", me, selfAnnouncementPath);
    announcer.unannounce(leadershipPath);
  }

  @Override
  public void announceSelf()
  {

    log.info("Announcing self [%s] at [%s]", me, selfAnnouncementPath);
    try {
      final byte[] meta = objectMapper.writeValueAsBytes(me);
      announcer.announce(selfAnnouncementPath, meta, false);
      for (String cap : capabilities) {
        announcer.announce(zkPathsConfig.getCapabilityPathForServerWithCapability(me, cap), meta, false);
      }
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void unannounceSelf()
  {
    announcer.unannounce(selfAnnouncementPath);
    for (String cap : capabilities) {
      announcer.unannounce(zkPathsConfig.getCapabilityPathFor(cap));
    }
  }
}
