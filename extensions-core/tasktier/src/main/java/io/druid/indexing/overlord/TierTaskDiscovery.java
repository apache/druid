/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.indexing.overlord.config.TierForkZkConfig;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TierTaskDiscovery
{
  private static final Logger log = new Logger(TierTaskDiscovery.class);
  private final CuratorFramework cf;
  private final TierForkZkConfig tierForkZkConfig;
  private final ObjectMapper mapper;
  private final String zkPath;

  @Inject
  public TierTaskDiscovery(
      TierForkZkConfig tierForkZkConfig,
      CuratorFramework cf,
      @Json ObjectMapper mapper
  )
  {
    this.tierForkZkConfig = tierForkZkConfig;
    this.cf = cf;
    this.mapper = mapper;
    this.zkPath = tierForkZkConfig.getTierTaskIDPath();
  }

  public List<String> getTaskIDs()
  {
    try {
      return cf.getChildren().forPath(zkPath);
    }
    catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
      log.info("No node at [%s] for task ids", zkPath);
      return ImmutableList.of();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Optional<DruidNode> getNodeForTask(String taskId)
  {
    final String path = tierForkZkConfig.getTierTaskIDPath(taskId);
    try {
      final byte[] data = cf.getData().decompressed().forPath(path);
      try {
        return Optional.of(mapper.readValue(data, DruidNode.class));
      }
      catch (IOException e) {
        log.warn(e, "Error reading data from [%s]", path);
        return Optional.absent();
      }
    }
    catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
      return Optional.absent();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Map<String, DruidNode> getTasks()
  {
    final Map<String, DruidNode> map = new HashMap<>();
    try {
      for (String taskId : cf.getChildren().forPath(tierForkZkConfig.getTierTaskIDPath())) {
        final String child = tierForkZkConfig.getTierTaskIDPath(taskId);
        log.debug("Checking [%s]", child);
        try {
          final byte[] data = cf.getData().decompressed().forPath(child);
          if (data == null) {
            log.debug("Null data at [%s]", child);
            continue;
          }
          final DruidNode node = mapper.readValue(data, DruidNode.class);
          map.put(taskId, node);
        }
        catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
          log.warn("Node vanished at [%s]", child); // Skip
        }
        catch (IOException e) {
          log.error(e, "Failed to parse node data for [%s] at [%s]", taskId, child);
        }
        catch (Exception e) {
          log.error(e, "Error fetching data for node [%s]", child);
        }
      }
    }
    catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
      log.debug("No node at [%s]", zkPath);
      return ImmutableMap.of();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return ImmutableMap.copyOf(map);
  }
}
