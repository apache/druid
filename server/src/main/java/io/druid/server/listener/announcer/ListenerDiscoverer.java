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

package io.druid.server.listener.announcer;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;

import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ListenerDiscoverer
{
  private static final Logger LOG = new Logger(ListenerDiscoverer.class);
  private volatile Map<HostAndPort, Long> lastSeenMap = ImmutableMap.of();
  private final CuratorFramework cf;
  private final ListeningAnnouncerConfig listeningAnnouncerConfig;
  private final Object startStopSync = new Object();
  private volatile boolean started = false;

  @Inject
  public ListenerDiscoverer(
      CuratorFramework cf,
      ListeningAnnouncerConfig listeningAnnouncerConfig
  )
  {
    this.cf = cf;
    this.listeningAnnouncerConfig = listeningAnnouncerConfig;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (startStopSync) {
      if (started) {
        LOG.debug("Already started");
        return;
      }
      started = true;
      LOG.info("Started");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopSync) {
      if (!started) {
        LOG.debug("Already stopped");
        return;
      }
      LOG.info("Stopped");
      started = false;
    }
  }

  /**
   * Get nodes at a particular listener.
   * This method lazily adds service discovery
   *
   * @param listener_key The Listener's service key
   *
   * @return A collection of druid nodes as established by the service discovery
   *
   * @throws IOException if there was an error refreshing the zookeeper cache
   */
  public Collection<HostAndPort> getNodes(final String listener_key) throws IOException
  {
    return getCurrentNodes(listener_key).keySet();
  }

  Map<HostAndPort, Long> getCurrentNodes(final String listener_key) throws IOException
  {
    final HashMap<HostAndPort, Long> retVal = new HashMap<>();
    final String zkPath = listeningAnnouncerConfig.getAnnouncementPath(listener_key);
    final Collection<String> children;
    try {
      children = cf.getChildren().forPath(zkPath);
    }
    catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
      LOG.debug(e, "No path found at [%s]", zkPath);
      return ImmutableMap.of();
    }
    catch (Exception e) {
      throw new IOException("Error getting children for " + zkPath, e);
    }
    for (String child : children) {
      final String childPath = ZKPaths.makePath(zkPath, child);
      try {
        final byte[] data;
        try {
          data = cf.getData().decompressed().forPath(childPath);
        }
        catch (Exception e) {
          throw new IOException("Error getting data for " + childPath, e);
        }
        if (data == null) {
          LOG.debug("Lost data at path [%s]", childPath);
          continue;
        }
        final HostAndPort hostAndPort = HostAndPort.fromString(child);
        final Long l = ByteBuffer.wrap(data).getLong();
        retVal.put(hostAndPort, l);
      }
      catch (IllegalArgumentException iae) {
        LOG.warn(iae, "Error parsing [%s]", childPath);
      }
    }
    return ImmutableMap.copyOf(retVal);
  }

  /**
   * Get only nodes that are new since the last time getNewNodes was called (or all nodes if it has never been called)
   *
   * @param listener_key The listener key to look for
   *
   * @return A collection of nodes that are new
   *
   * @throws IOException If there was an error in refreshing the Zookeeper cache
   */
  public synchronized Collection<HostAndPort> getNewNodes(final String listener_key) throws IOException
  {
    final Map<HostAndPort, Long> priorSeenMap = lastSeenMap;
    final Map<HostAndPort, Long> currentMap = getCurrentNodes(listener_key);
    final Collection<HostAndPort> retVal = Collections2.filter(
        currentMap.keySet(),
        new Predicate<HostAndPort>()
        {
          @Override
          public boolean apply(HostAndPort input)
          {
            final Long l = priorSeenMap.get(input);
            return l == null || l < currentMap.get(input);
          }
        }
    );
    lastSeenMap = currentMap;
    return retVal;
  }

  /**
   * Discovers children of the listener key
   *
   * @param key_base The base of the listener key, or null or empty string to get all immediate children of the listener path
   *
   * @return A collection of the names of the children, or empty list on NoNodeException from Curator
   *
   * @throws IOException      from Curator
   * @throws RuntimeException for other exceptions from Curator.
   */
  public Collection<String> discoverChildren(@Nullable final String key_base) throws IOException
  {
    final String zkPath = Strings.isNullOrEmpty(key_base)
                          ? listeningAnnouncerConfig.getListenersPath()
                          : listeningAnnouncerConfig.getAnnouncementPath(key_base);
    try {
      return cf.getChildren().forPath(zkPath);
    }
    catch (KeeperException.NoNodeException | KeeperException.NoChildrenForEphemeralsException e) {
      LOG.warn(e, "Path [%s] not discoverable", zkPath);
      return ImmutableList.of();
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }
}
