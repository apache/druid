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

package io.druid.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;

public class CuratorUtils
{
  public static final int DEFAULT_MAX_ZNODE_BYTES = 512 * 1024;

  private static final Logger log = new Logger(CuratorUtils.class);

  /**
   * Create znode if it does not already exist. If it does already exist, this does nothing. In particular, the
   * existing znode may have a different payload or create mode.
   *
   * @param curatorFramework curator
   * @param path             path
   * @param mode             create mode
   * @param rawBytes         payload
   * @param maxZnodeBytes    maximum payload size
   *
   * @throws IllegalArgumentException if rawBytes.length > maxZnodeBytes
   * @throws Exception                if Curator throws an Exception
   */
  public static void createIfNotExists(
      CuratorFramework curatorFramework,
      String path,
      CreateMode mode,
      byte[] rawBytes,
      int maxZnodeBytes
  ) throws Exception
  {
    verifySize(path, rawBytes, maxZnodeBytes);

    if (curatorFramework.checkExists().forPath(path) == null) {
      try {
        curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(mode)
                        .forPath(path, rawBytes);
      }
      catch (KeeperException.NodeExistsException e) {
        log.info("Skipping create path[%s], since it already exists.", path);
      }
    }
  }

  /**
   * Create znode if it does not already exist. If it does already exist, update the payload (but not the create mode).
   * If someone deletes the znode while we're trying to set it, just let it stay deleted.
   *
   * @param curatorFramework curator
   * @param path             path
   * @param mode             create mode
   * @param rawBytes         payload
   * @param maxZnodeBytes    maximum payload size
   *
   * @throws IllegalArgumentException if rawBytes.length > maxZnodeBytes
   * @throws Exception                if Curator throws an Exception
   */
  public static void createOrSet(
      CuratorFramework curatorFramework,
      String path,
      CreateMode mode,
      byte[] rawBytes,
      int maxZnodeBytes
  ) throws Exception
  {
    verifySize(path, rawBytes, maxZnodeBytes);

    boolean created = false;
    if (curatorFramework.checkExists().forPath(path) == null) {
      try {
        curatorFramework.create()
                        .creatingParentsIfNeeded()
                        .withMode(mode)
                        .forPath(path, rawBytes);

        created = true;
      }
      catch (KeeperException.NodeExistsException e) {
        log.debug("Path [%s] created while we were running, will setData instead.", path);
      }
    }

    if (!created) {
      try {
        curatorFramework.setData()
                        .forPath(path, rawBytes);
      }
      catch (KeeperException.NoNodeException e) {
        log.warn("Someone deleted path[%s] while we were trying to set it. Leaving it deleted.", path);
      }
    }
  }

  private static void verifySize(String path, byte[] rawBytes, int maxZnodeBytes)
  {
    if (rawBytes.length > maxZnodeBytes) {
      throw new IAE(
          "Length of raw bytes for znode[%s] too large[%,d > %,d]",
          path,
          rawBytes.length,
          maxZnodeBytes
      );
    }
  }
}
