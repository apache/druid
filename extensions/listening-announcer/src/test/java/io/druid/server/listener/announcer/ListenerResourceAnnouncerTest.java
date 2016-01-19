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

import io.druid.curator.CuratorTestBase;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ListenerResourceAnnouncerTest extends CuratorTestBase
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testAnnouncerBehaves() throws Exception
  {
    setupServerAndCurator();
    final String listener_key = "listener_key";
    final ListeningAnnouncerConfig config = new ListeningAnnouncerConfig(new ZkPathsConfig());
    curator.start();
    try {
      Assert.assertTrue(curator.blockUntilConnected(10, TimeUnit.SECONDS));
      final DruidNode node = new DruidNode("test_service", "localhost", -1);
      final ListenerResourceAnnouncer listenerResourceAnnouncer = new ListenerResourceAnnouncer(
          curator,
          config,
          listener_key,
          node
      )
      {
      };
      listenerResourceAnnouncer.start();
      final String path = config.getAnnouncementPath(listener_key);
      Assert.assertNotNull(curator.checkExists().forPath(path));
      Assert.assertNull(curator.checkExists().forPath(config.getAnnouncementPath(listener_key + "FOO")));
      listenerResourceAnnouncer.stop();
      listenerResourceAnnouncer.start();
      listenerResourceAnnouncer.start();
      listenerResourceAnnouncer.stop();
      listenerResourceAnnouncer.stop();
      listenerResourceAnnouncer.start();
      listenerResourceAnnouncer.stop();
      listenerResourceAnnouncer.start();
      listenerResourceAnnouncer.stop();
      // Curator atomicity sucks
      // Assert.assertNull(curator.checkExists().forPath(path));
    }
    finally {
      try {
        curator.close();
      }
      finally {
        tearDownServerAndCurator();
      }
    }
    Assert.assertEquals(CuratorFrameworkState.STOPPED, curator.getState());
  }

  @Test
  public void testStartCorrect() throws Exception
  {
    final ServiceDiscovery<Void> discovery = EasyMock.createStrictMock(ServiceDiscovery.class);
    final String listener_key = "listener_key_thing";
    final DruidNode node = new DruidNode("some_service", "some_host", -1);

    Capture<ServiceInstance<Void>> serviceInstanceCapture = Capture.newInstance();
    discovery.start();
    EasyMock.expectLastCall().once();
    discovery.registerService(EasyMock.capture(serviceInstanceCapture));
    EasyMock.expectLastCall().once();
    EasyMock.replay(discovery);

    final ListenerResourceAnnouncer resourceAnnouncer = new ListenerResourceAnnouncer(
        discovery,
        listener_key,
        node
    )
    {
    };
    resourceAnnouncer.start();
    Assert.assertTrue(serviceInstanceCapture.hasCaptured());
    final ServiceInstance<Void> capturedService = serviceInstanceCapture.getValue();
    Assert.assertEquals(node.getHost(), capturedService.getAddress());
    Assert.assertEquals(node.getPort(), (int) capturedService.getPort());
    Assert.assertEquals(listener_key, capturedService.getName());
    EasyMock.verify(discovery);
  }

  @Test
  public void testStopError() throws Exception
  {
    final ServiceDiscovery<Void> discovery = EasyMock.createStrictMock(ServiceDiscovery.class);
    final String listener_key = "listener_key_thing";
    final DruidNode node = new DruidNode("some_service", "some_host", -1);
    final IOException ex = new IOException("test exception");


    discovery.start();
    EasyMock.expectLastCall().once();
    discovery.registerService(EasyMock.<ServiceInstance<Void>>anyObject());
    EasyMock.expectLastCall().once();
    discovery.close();
    EasyMock.expectLastCall().andThrow(ex).once();

    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    EasyMock.replay(discovery);

    final ListenerResourceAnnouncer resourceAnnouncer = new ListenerResourceAnnouncer(
        discovery,
        listener_key,
        node
    )
    {
    };

    try {
      resourceAnnouncer.start();
      resourceAnnouncer.stop();
    }
    finally {
      EasyMock.verify(discovery);
    }
  }

  @Test
  public void testStartError() throws Exception
  {
    final ServiceDiscovery<Void> discovery = EasyMock.createStrictMock(ServiceDiscovery.class);
    final String listener_key = "listener_key_thing";
    final DruidNode node = new DruidNode("some_service", "some_host", -1);
    final RuntimeException ex = new RuntimeException("test exception");

    discovery.start();
    EasyMock.expectLastCall().andThrow(ex).once();
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    EasyMock.replay(discovery);
    final ListenerResourceAnnouncer resourceAnnouncer = new ListenerResourceAnnouncer(
        discovery,
        listener_key,
        node
    )
    {
    };
    try {
      resourceAnnouncer.start();
    }
    finally {
      EasyMock.verify(discovery);
    }
  }
}
