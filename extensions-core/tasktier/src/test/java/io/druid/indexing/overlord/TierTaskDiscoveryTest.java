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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.curator.announcement.Announcer;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.annotations.Json;
import io.druid.indexing.overlord.config.TierForkZkConfig;
import io.druid.initialization.Initialization;
import io.druid.segment.CloserRule;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class TierTaskDiscoveryTest
{
  @Rule
  public CloserRule closerRule = new CloserRule(true);
  private final TierForkZkConfig tierForkZkConfig = new TierForkZkConfig(new ZkPathsConfig(), null, null);
  private TestingServer zkTestServer;
  private TierTaskDiscovery tierTaskDiscovery;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp() throws Exception
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
                    "testService"
                );
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
                binder.bind(TaskRunner.class).toInstance(EasyMock.createNiceMock(TaskRunner.class));
                binder.bind(TaskStorage.class).toInstance(EasyMock.createNiceMock(TaskStorage.class));
                binder.bind(TaskMaster.class).toInstance(EasyMock.createNiceMock(TaskMaster.class));
              }
            }
        )
    );
    jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final PortFinder portFinder = new PortFinder(8000);
    zkTestServer = new TestingServer(portFinder.findUnusedPort());
    zkTestServer.start();
    closerRule.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            zkTestServer.stop();
          }
        }
    );
    closerRule.closeLater(zkTestServer);
    final CuratorFramework framework = CuratorFrameworkFactory.newClient(
        String.format(
            "localhost:%d",
            zkTestServer.getPort()
        ), new RetryOneTime(10)
    );
    framework.start();
    framework.inTransaction().create().forPath("/" + tierForkZkConfig.zkPathsConfig.getBase()).and().commit();
    tierTaskDiscovery = new TierTaskDiscovery(
        tierForkZkConfig,
        closerRule.closeLater(framework),
        jsonMapper
    );
  }

  @Test
  public void testSimpleDiscoNoNode()
  {
    Assert.assertTrue(tierTaskDiscovery.getTaskIDs().isEmpty());
    Assert.assertTrue(tierTaskDiscovery.getTasks().isEmpty());
    Assert.assertFalse(tierTaskDiscovery.getNodeForTask("does not exist").isPresent());
  }


  @Test
  public void testSimpleDiscoEmptyNode() throws Exception
  {
    final CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
        String.format(
            "localhost:%d",
            zkTestServer.getPort()
        ), new RetryOneTime(10)
    );
    curatorFramework.start();
    closerRule.closeLater(curatorFramework);
    curatorFramework.blockUntilConnected();

    curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(tierForkZkConfig.getTierTaskIDPath());

    Assert.assertTrue(tierTaskDiscovery.getTaskIDs().isEmpty());
    Assert.assertTrue(tierTaskDiscovery.getTasks().isEmpty());
    Assert.assertFalse(tierTaskDiscovery.getNodeForTask("does not exist").isPresent());
  }

  @Test
  public void testDisco() throws Exception
  {
    final ExecutorService service = Execs.singleThreaded("testAnnouncer");
    final CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
        String.format(
            "localhost:%d",
            zkTestServer.getPort()
        ), new RetryOneTime(10)
    );
    curatorFramework.start();
    closerRule.closeLater(curatorFramework);
    curatorFramework.blockUntilConnected();

    final String host = "somehost";
    final String taskId = "taskId";
    final String type = "sometier";
    final int port = 9898;
    final DruidNode testNode = new DruidNode(type, host, port);
    final byte[] bytes = StringUtils.toUtf8(jsonMapper.writeValueAsString(testNode));

    final Announcer announcer = new Announcer(curatorFramework, service);
    announcer.start();
    closerRule.closeLater(
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            announcer.stop();
          }
        }
    );
    announcer.announce(tierForkZkConfig.getTierTaskIDPath(taskId), bytes);

    while (tierTaskDiscovery.getTasks().isEmpty()) {
      Thread.sleep(100);
    }

    Assert.assertEquals(ImmutableList.of(taskId), ImmutableList.copyOf(tierTaskDiscovery.getTaskIDs()));
    Assert.assertEquals(testNode, tierTaskDiscovery.getTasks().get(taskId));
    Assert.assertEquals(testNode, tierTaskDiscovery.getNodeForTask(taskId).get());
  }
}
