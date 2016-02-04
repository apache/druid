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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Closer;
import io.druid.curator.CuratorTestBase;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceCacheBuilder;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadFactory;

public class ListenerDiscovererTest extends CuratorTestBase
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private final ServiceDiscovery<Void> serviceDiscovery = EasyMock.createStrictMock(ServiceDiscovery.class);
  private final String listen_key = "listener_key";

  @Test
  public void testGetNodes() throws Exception
  {
    final ServiceCache<Void> serviceCache = EasyMock.createStrictMock(ServiceCache.class);
    final ServiceCache<Void> serviceCacheEmpty = EasyMock.createStrictMock(ServiceCache.class);
    final ServiceCacheBuilder<Void> serviceCacheBuilder = EasyMock.createStrictMock(ServiceCacheBuilder.class);
    final ServiceCacheBuilder<Void> serviceCacheBuilderEmpty = EasyMock.createStrictMock(ServiceCacheBuilder.class);
    final ListenerDiscoverer listenerDiscoverer = new ListenerDiscoverer(serviceDiscovery);

    // Lazy
    serviceDiscovery.start();
    EasyMock.expectLastCall().once();
    EasyMock.replay(serviceDiscovery);
    listenerDiscoverer.start();
    EasyMock.verify(serviceDiscovery);

    final String host = "local.hostname";
    final int port = 999;
    final ServiceInstance<Void> serviceInstance = ServiceInstance.<Void>builder().address(host)
                                                                                 .port(port)
                                                                                 .name(listen_key)
                                                                                 .build();
    final List<ServiceInstance<Void>> serviceInstanceList = ImmutableList.of(serviceInstance);

    serviceCache.start();
    EasyMock.expectLastCall().once();
    EasyMock.expect(serviceCache.getInstances()).andReturn(serviceInstanceList).once();


    EasyMock.expect(serviceCacheBuilder.name(EasyMock.eq(listen_key))).andReturn(serviceCacheBuilder).once();
    EasyMock.expect(serviceCacheBuilder.threadFactory(EasyMock.<ThreadFactory>anyObject()))
            .andReturn(serviceCacheBuilder)
            .once();
    EasyMock.expect(serviceCacheBuilder.build()).andReturn(serviceCache).once();


    final String not_listen_key = "NOTlistener_key";
    EasyMock.expect(serviceCacheBuilder.name(EasyMock.eq(not_listen_key))).andReturn(serviceCacheBuilderEmpty).once();
    EasyMock.expect(serviceCacheBuilderEmpty.threadFactory(EasyMock.<ThreadFactory>anyObject()))
            .andReturn(serviceCacheBuilderEmpty)
            .once();
    EasyMock.expect(serviceCacheBuilderEmpty.build()).andReturn(serviceCacheEmpty).once();
    serviceCacheEmpty.start();
    EasyMock.expectLastCall().once();
    EasyMock.expect(serviceCacheEmpty.getInstances()).andReturn(ImmutableList.<ServiceInstance<Void>>of()).once();

    EasyMock.reset(serviceDiscovery);
    EasyMock.expect(serviceDiscovery.serviceCacheBuilder()).andReturn(serviceCacheBuilder).times(2);

    EasyMock.replay(serviceDiscovery, serviceCacheBuilder, serviceCache, serviceCacheBuilderEmpty, serviceCacheEmpty);

    final Collection<DruidNode> nodes = listenerDiscoverer.getNodes(listen_key);
    final Collection<DruidNode> no_nodes = listenerDiscoverer.getNodes(not_listen_key);

    EasyMock.verify(serviceDiscovery, serviceCacheBuilder, serviceCache, serviceCacheBuilderEmpty, serviceCacheEmpty);

    Assert.assertEquals(1, nodes.size());
    final DruidNode node = Iterables.getFirst(nodes, null);
    Assert.assertNotNull(node);
    Assert.assertEquals(host, node.getHost());
    Assert.assertEquals(port, node.getPort());

    Assert.assertTrue(no_nodes.isEmpty());
  }


  @Test
  public void testStartError() throws Exception
  {
    final RuntimeException ex = new RuntimeException("test exception");
    serviceDiscovery.start();
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
    EasyMock.replay(serviceDiscovery);
    final ListenerDiscoverer listenerDiscoverer = new ListenerDiscoverer(serviceDiscovery);
    listenerDiscoverer.start();
    EasyMock.verify(serviceDiscovery);
  }


  @Test
  public void testStopError() throws Exception
  {
    final IOException ex = new IOException("test exception");
    serviceDiscovery.start();
    EasyMock.expectLastCall().once();
    serviceDiscovery.close();
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
    EasyMock.replay(serviceDiscovery);
    final ListenerDiscoverer listenerDiscoverer = new ListenerDiscoverer(serviceDiscovery);
    listenerDiscoverer.start();
    listenerDiscoverer.stop();
    EasyMock.verify(serviceDiscovery);
  }

  @Test(timeout = 5_000)
  public void testFullService() throws Exception
  {
    setupServerAndCurator();
    final Closer closer = Closer.create();
    try {
      closer.register(server);
      closer.register(curator);
      curator.start();
      curator.blockUntilConnected();
      final ListeningAnnouncerConfig config = new ListeningAnnouncerConfig(new ZkPathsConfig());
      final ListenerDiscoverer listenerDiscoverer = new ListenerDiscoverer(curator, config);
      listenerDiscoverer.start();
      closer.register(new Closeable()
      {
        @Override
        public void close() throws IOException
        {
          listenerDiscoverer.stop();
        }
      });
      Assert.assertTrue(listenerDiscoverer.getNodes(listen_key).isEmpty());

      final DruidNode node = new DruidNode(listen_key, "someHost", 8888);
      final ListenerResourceAnnouncer listenerResourceAnnouncer = new ListenerResourceAnnouncer(
          curator,
          config,
          listen_key,
          node
      )
      {
      };
      listenerResourceAnnouncer.start();
      closer.register(new Closeable()
      {
        @Override
        public void close() throws IOException
        {
          listenerResourceAnnouncer.stop();
        }
      });

      // Have to wait for background syncing
      while (listenerDiscoverer.getNodes(listen_key).isEmpty()) {
        // Will timeout at test's timeout setting
        Thread.sleep(1);
      }
      Assert.assertEquals(ImmutableList.of(node), listenerDiscoverer.getNodes(listen_key));
    }
    catch (Throwable t) {
      throw closer.rethrow(t);
    }
    finally {
      closer.close();
    }
  }
}
