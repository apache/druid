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

package io.druid.indexer.lock;


import io.druid.java.util.common.logger.Logger;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;

import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;


import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class ZookeeperDistributedLock implements DistributedLock
{
  private static final Logger log = new Logger(ZookeeperDistributedLock.class);

  public static class Factory
  {

    private static final ConcurrentMap<String, CuratorFramework> CACHE = new ConcurrentHashMap<String, CuratorFramework>();

    static {
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          for (CuratorFramework curator : CACHE.values()) {
            try {
              curator.close();
            }
            catch (Exception ex) {
              log.error("Error at closing " + curator, ex);
            }
          }
        }
      }));
    }

    private static CuratorFramework getZKClient(String config)
    {
      CuratorFramework zkClient = CACHE.get(config);
      if (zkClient == null) {
        synchronized (ZookeeperDistributedLock.class) {
          zkClient = CACHE.get(config);
          if (zkClient == null) {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            String zkConnectString = config;
            zkClient = CuratorFrameworkFactory.builder()
                                              .connectString(zkConnectString)
                                              .sessionTimeoutMs(120000)
                                              .connectionTimeoutMs(15000)
                                              .retryPolicy(retryPolicy)
                                              .build();
            zkClient.start();
            CACHE.put(config, zkClient);
            if (CACHE.size() > 1) {
              log.warn("More than one singleton exist");
            }
          }
        }
      }
      return zkClient;
    }

    final String zkPathBase;
    final CuratorFramework curator;

    public Factory(String zkHosts, String zkBase)
    {
      this.curator = getZKClient(zkHosts);
      this.zkPathBase = fixSlash(zkBase + "/" + "global_lock");
    }

    public DistributedLock lockForClient(String client)
    {
      return new ZookeeperDistributedLock(curator, zkPathBase, client);
    }

    public DistributedLock lockForCurrentThread()
    {
      return lockForClient(threadProcessAndHost());
    }

    private static String threadProcessAndHost()
    {
      return Thread.currentThread().getId() + "-" + processAndHost();
    }

    private static String processAndHost()
    {
      byte[] bytes = ManagementFactory.getRuntimeMXBean().getName().getBytes();
      return new String(bytes);
    }
  }

  // ============================================================================

  final CuratorFramework curator;
  final String zkPathBase;
  final String client;
  final byte[] clientBytes;

  private ZookeeperDistributedLock(CuratorFramework curator, String zkPathBase, String client)
  {
    if (client == null) {
      throw new NullPointerException("client must not be null");
    }
    if (zkPathBase == null) {
      throw new NullPointerException("zkPathBase must not be null");
    }

    this.curator = curator;
    this.zkPathBase = zkPathBase;
    this.client = client;
    this.clientBytes = client.getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public String getClient()
  {
    return client;
  }

  @Override
  public boolean lock(String lockPath)
  {
    lockPath = norm(lockPath);

    log.debug(client + " trying to lock " + lockPath);

    try {
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockPath, clientBytes);
    }
    catch (KeeperException.NodeExistsException ex) {
      log.debug(client + " see " + lockPath + " is already locked");
    }
    catch (Exception ex) {
      throw new RuntimeException("Error while " + client + " trying to lock " + lockPath, ex);
    }

    String lockOwner = peekLock(lockPath);
    if (client.equals(lockOwner)) {
      log.info(client + " acquired lock at " + lockPath);
      return true;
    } else {
      log.debug(client + " failed to acquire lock at " + lockPath + ", which is held by " + lockOwner);
      return false;
    }
  }

  @Override
  public boolean lock(String lockPath, long timeout)
  {
    lockPath = norm(lockPath);

    if (lock(lockPath)) {
      return true;
    }

    if (timeout <= 0) {
      timeout = Long.MAX_VALUE;
    }

    log.debug(client + " will wait for lock path " + lockPath);
    long waitStart = System.currentTimeMillis();
    Random random = new Random();
    long sleep = 10 * 1000; // 10 seconds

    while (System.currentTimeMillis() - waitStart <= timeout) {
      try {
        Thread.sleep((long) (1000 + sleep * random.nextDouble()));
      }
      catch (InterruptedException e) {
        return false;
      }

      if (lock(lockPath)) {
        log.debug(client + " waited " + (System.currentTimeMillis() - waitStart) + " ms for lock path " + lockPath);
        return true;
      }
    }

    // timeout
    return false;
  }

  @Override
  public String peekLock(String lockPath)
  {
    lockPath = norm(lockPath);

    try {
      byte[] bytes = curator.getData().forPath(lockPath);
      return new String(bytes, Charset.forName("UTF-8"));
    }
    catch (KeeperException.NoNodeException ex) {
      return null;
    }
    catch (Exception ex) {
      throw new RuntimeException("Error while peeking at " + lockPath, ex);
    }
  }

  @Override
  public boolean isLocked(String lockPath)
  {
    return peekLock(lockPath) != null;
  }

  @Override
  public boolean isLockedByMe(String lockPath)
  {
    return client.equals(peekLock(lockPath));
  }

  @Override
  public void unlock(String lockPath)
  {
    lockPath = norm(lockPath);

    log.debug(client + " trying to unlock " + lockPath);

    String owner = peekLock(lockPath);
    if (owner == null) {
      throw new IllegalStateException(client + " cannot unlock path " + lockPath + " which is not locked currently");
    }
    if (client.equals(owner) == false) {
      throw new IllegalStateException(client + " cannot unlock path " + lockPath + " which is locked by " + owner);
    }

    try {
      curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPath);

      log.info(client + " released lock at " + lockPath);

    }
    catch (Exception ex) {
      throw new RuntimeException("Error while " + client + " trying to unlock " + lockPath, ex);
    }
  }

  @Override
  public void purgeLocks(String lockPathRoot)
  {
    lockPathRoot = norm(lockPathRoot);

    try {
      curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(lockPathRoot);

      log.info(client + " purged all locks under " + lockPathRoot);

    }
    catch (Exception ex) {
      throw new RuntimeException("Error while " + client + " trying to purge " + lockPathRoot, ex);
    }
  }

  // normalize lock path
  private String norm(String lockPath)
  {
    if (!lockPath.startsWith(zkPathBase)) {
      lockPath = zkPathBase + (lockPath.startsWith("/") ? "" : "/") + lockPath;
    }
    return fixSlash(lockPath);
  }

  private static String fixSlash(String path)
  {
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    for (int n = Integer.MAX_VALUE; n > path.length();) {
      n = path.length();
      path = path.replace("//", "/");
    }
    return path;
  }
}
