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

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Lists;
import com.google.api.client.util.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class ZookeeperDruidServerDiscovery implements DruidServerDiscovery
{

  private final CuratorFramework curator;
  private final ObjectMapper objectMapper;
  private final ZkPathsConfig zkPathsConfig;

  @Inject
  public ZookeeperDruidServerDiscovery(CuratorFramework curator, ObjectMapper objectMapper, ZkPathsConfig zkPathsConfig)
  {
    this.curator = curator;
    this.objectMapper = objectMapper;
    this.zkPathsConfig = zkPathsConfig;
  }

  @Override
  public List<DruidServerMetadata> getServersForType(String type)
  {
    return getNodesUnderPath(zkPathsConfig.getAnnouncementPathForType(type));
  }

  @Override
  public DruidServerMetadata getLeaderForType(String type)
  {
    final List<DruidServerMetadata> leader = getNodesUnderPath(zkPathsConfig.getLeadershipPathForType(type));
    if (leader.size() > 1) {
      throw new ISE("There should only be 1 Coordinator leader in the cluster, got [%s]", leader);
    }
    return leader.size() == 0 ? null : leader.get(0);
  }

  @Override
  public List<DruidServerMetadata> getServersForTypeWithService(final String type, final String service)
  {
    final List<DruidServerMetadata> retVal = new ArrayList<>();
    for (DruidServerMetadata server : getServersForType(type)) {
      if (server.getService().equals(service)) {
        retVal.add(server);
      }
    }
    return retVal;
  }

  @Override
  public List<DruidServerMetadata> getServersWithCapability(String capability)
  {
    return getNodesUnderPath(zkPathsConfig.getCapabilityPathFor(capability));
  }

  private List<DruidServerMetadata> getNodesUnderPath(String announcementPath)
  {
    final ImmutableList.Builder<DruidServerMetadata> retVal = ImmutableList.builder();

    try {
      final List<String> children = curator.getChildren().forPath(announcementPath);
      for (String hostName : children) {

        retVal.add(objectMapper.readValue(
            curator.getData()
                   .decompressed()
                   .forPath(ZKPaths.makePath(announcementPath, hostName)),
            DruidServerMetadata.class
        ));
      }
    }
    catch (KeeperException.NoNodeException e) {
      return Lists.newArrayList();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return retVal.build();
  }
}
