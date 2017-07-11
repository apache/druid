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

package io.druid.server.listener.announcer;

import com.google.common.collect.ImmutableSet;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.announcement.Announcer;
import io.druid.segment.CloserRule;
import io.druid.server.http.HostAndPortWithScheme;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ListenerDiscovererTest extends CuratorTestBase
{
  @Rule
  public CloserRule closerRule = new CloserRule(true);

  @Test(timeout = 60_000L)
  public void testFullService() throws Exception
  {
    final String listenerKey = "listenerKey";
    final String listenerTier = "listenerTier";
    final String listenerTierChild = "tierChild";
    final String tierZkPath = ZKPaths.makePath(listenerTier, listenerTierChild);

    setupServerAndCurator();
    final ExecutorService executorService = Execs.singleThreaded("listenerDiscovererTest--%s");
    closerRule.closeLater(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        executorService.shutdownNow();
      }
    });
    closerRule.closeLater(server);
    closerRule.closeLater(curator);
    curator.start();
    curator.blockUntilConnected(10, TimeUnit.SECONDS);
    Assert.assertEquals("/druid", curator.create().forPath("/druid"));
    final Announcer announcer = new Announcer(curator, executorService);
    closerRule.closeLater(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        announcer.stop();
      }
    });
    final ListeningAnnouncerConfig config = new ListeningAnnouncerConfig(new ZkPathsConfig());
    final ListenerDiscoverer listenerDiscoverer = new ListenerDiscoverer(curator, config);
    listenerDiscoverer.start();
    closerRule.closeLater(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        listenerDiscoverer.stop();
      }
    });
    Assert.assertTrue(listenerDiscoverer.getNodes(listenerKey).isEmpty());

    final HostAndPortWithScheme node = HostAndPortWithScheme.fromParts("http", "someHost", 8888);
    final ListenerResourceAnnouncer listenerResourceAnnouncer = new ListenerResourceAnnouncer(
        announcer,
        config,
        listenerKey,
        node
    )
    {
    };
    listenerResourceAnnouncer.start();
    closerRule.closeLater(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        listenerResourceAnnouncer.stop();
      }
    });

    final ListenerResourceAnnouncer tieredListenerResourceAnnouncer = new ListenerResourceAnnouncer(
        announcer,
        config,
        tierZkPath,
        node
    )
    {
    };
    tieredListenerResourceAnnouncer.start();
    closerRule.closeLater(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        tieredListenerResourceAnnouncer.stop();
      }
    });

    announcer.start();

    Assert.assertNotNull(curator.checkExists().forPath(config.getAnnouncementPath(listenerKey)));
    // Have to wait for background syncing
    while (listenerDiscoverer.getNodes(listenerKey).isEmpty()) {
      // Will timeout at test's timeout setting
      Thread.sleep(1);
    }
    Assert.assertEquals(
        ImmutableSet.of(HostAndPortWithScheme.fromString(node.toString())),
        listenerDiscoverer.getNodes(listenerKey)
    );
    // 2nd call of two concurrent getNewNodes should return no entry collection
    listenerDiscoverer.getNewNodes(listenerKey);
    Assert.assertEquals(
        0,
        listenerDiscoverer.getNewNodes(listenerKey).size()
    );
    Assert.assertEquals(
        ImmutableSet.of(listenerKey, listenerTier),
        ImmutableSet.copyOf(listenerDiscoverer.discoverChildren(null))
    );
    Assert.assertEquals(
        ImmutableSet.of(listenerTierChild),
        ImmutableSet.copyOf(listenerDiscoverer.discoverChildren(listenerTier))
    );
  }
}
