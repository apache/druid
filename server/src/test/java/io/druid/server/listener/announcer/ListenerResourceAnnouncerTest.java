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

import com.google.common.primitives.Longs;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.curator.announcement.Announcer;
import io.druid.segment.CloserRule;
import io.druid.server.http.HostAndPortWithScheme;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ListenerResourceAnnouncerTest extends CuratorTestBase
{
  private final ListeningAnnouncerConfig listeningAnnouncerConfig = new ListeningAnnouncerConfig(new ZkPathsConfig());
  private final String listenerKey = "someKey";
  private final String announcePath = listeningAnnouncerConfig.getAnnouncementPath(listenerKey);
  @Rule
  public CloserRule closerRule = new CloserRule(true);
  private ExecutorService executorService;

  @Before
  public void setUp()
  {
    executorService = Execs.singleThreaded("listener-resource--%d");
  }

  @After
  public void tearDown()
  {
    executorService.shutdownNow();
  }

  @Test
  public void testAnnouncerBehaves() throws Exception
  {
    setupServerAndCurator();
    closerRule.closeLater(server);
    curator.start();
    closerRule.closeLater(curator);
    Assert.assertNotNull(curator.create().forPath("/druid"));
    Assert.assertTrue(curator.blockUntilConnected(10, TimeUnit.SECONDS));
    final Announcer announcer = new Announcer(curator, executorService);
    final HostAndPortWithScheme node = HostAndPortWithScheme.fromString("localhost");
    final ListenerResourceAnnouncer listenerResourceAnnouncer = new ListenerResourceAnnouncer(
        announcer,
        listeningAnnouncerConfig,
        listenerKey,
        node
    )
    {
    };
    listenerResourceAnnouncer.start();
    announcer.start();
    closerRule.closeLater(new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        announcer.stop();
      }
    });
    Assert.assertNotNull(curator.checkExists().forPath(announcePath));
    final String nodePath = ZKPaths.makePath(announcePath, String.format("%s:%s", node.getScheme(), node.getHostText()));
    Assert.assertNotNull(curator.checkExists().forPath(nodePath));
    Assert.assertEquals(Longs.BYTES, curator.getData().decompressed().forPath(nodePath).length);
    Assert.assertNull(curator.checkExists()
                             .forPath(listeningAnnouncerConfig.getAnnouncementPath(listenerKey + "FOO")));
    listenerResourceAnnouncer.stop();
    listenerResourceAnnouncer.start();
    listenerResourceAnnouncer.start();
    listenerResourceAnnouncer.stop();
    listenerResourceAnnouncer.stop();
    listenerResourceAnnouncer.start();
    listenerResourceAnnouncer.stop();
    listenerResourceAnnouncer.start();
    listenerResourceAnnouncer.stop();
    Assert.assertNull(curator.checkExists().forPath(nodePath));
  }

  @Test
  public void testStartCorrect() throws Exception
  {
    final Announcer announcer = EasyMock.createStrictMock(Announcer.class);
    final HostAndPortWithScheme node = HostAndPortWithScheme.fromString("some_host");

    final ListenerResourceAnnouncer resourceAnnouncer = new ListenerResourceAnnouncer(
        announcer,
        listeningAnnouncerConfig,
        listenerKey,
        node
    )
    {
    };


    announcer.announce(
        EasyMock.eq(ZKPaths.makePath(announcePath, String.format("%s:%s", node.getScheme(), node.getHostText()))),
        EasyMock.aryEq(resourceAnnouncer.getAnnounceBytes())
    );
    EasyMock.expectLastCall().once();
    EasyMock.replay(announcer);
    resourceAnnouncer.start();
    EasyMock.verify(announcer);
  }
}
