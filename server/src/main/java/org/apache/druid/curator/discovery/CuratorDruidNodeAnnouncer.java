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

package org.apache.druid.curator.discovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;

/**
 */
public class CuratorDruidNodeAnnouncer implements DruidNodeAnnouncer
{
  private static final Logger log = new Logger(CuratorDruidNodeAnnouncer.class);

  private final Announcer announcer;
  private final ZkPathsConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public CuratorDruidNodeAnnouncer(
      Announcer announcer,
      ZkPathsConfig config,
      @Json ObjectMapper jsonMapper
  )
  {
    this.announcer = announcer;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void announce(DiscoveryDruidNode discoveryDruidNode)
  {
    try {
      log.info("Announcing [%s].", discoveryDruidNode);

      announcer.announce(
          ZKPaths.makePath(
              config.getInternalDiscoveryPath(),
              discoveryDruidNode.getNodeType(),
              discoveryDruidNode.getDruidNode().getHostAndPortToUse()
          ),
          jsonMapper.writeValueAsBytes(discoveryDruidNode)
      );

      log.info("Announced [%s].", discoveryDruidNode);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void unannounce(DiscoveryDruidNode discoveryDruidNode)
  {
    log.info("Unannouncing [%s].", discoveryDruidNode);

    announcer.unannounce(
        ZKPaths.makePath(
            config.getInternalDiscoveryPath(),
            discoveryDruidNode.getNodeType(),
            discoveryDruidNode.getDruidNode().getHostAndPortToUse()
        )
    );

    log.info("Unannounced [%s].", discoveryDruidNode);
  }
}
