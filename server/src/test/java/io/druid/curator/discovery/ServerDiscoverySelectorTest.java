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

package io.druid.curator.discovery;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.metamx.common.ISE;
import io.druid.client.DruidServerDiscovery;
import io.druid.client.selector.Server;
import io.druid.server.coordination.DruidServerMetadata;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;

public class ServerDiscoverySelectorTest
{

  private static final int PORT = 8080;
  private static final String ADDRESS = "localhost";

  private ServerDiscoverySelector serverDiscoverySelector;
  private DruidServerDiscovery discovery;

  @Before
  public void setUp() throws Exception
  {
    discovery = EasyMock.createMock(DruidServerDiscovery.class);
    serverDiscoverySelector = new ServerDiscoverySelector(
        discovery,
        new Function<DruidServerDiscovery, List<DruidServerMetadata>>()
        {
          @Override
          public List<DruidServerMetadata> apply(DruidServerDiscovery discovery)
          {
            try {
              return Lists.newArrayList(discovery.getLeaderForType("coordinator"));
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
  }

  @Test
  public void testPickCoordinator() throws Exception
  {
    EasyMock.expect(discovery.getLeaderForType("coordinator"))
            .andReturn(new DruidServerMetadata("coor", ADDRESS + ":" + PORT, 0, "coordinator", "t1", 0, "service",
                                               ADDRESS,
                                               PORT))
            .once();
    EasyMock.replay(discovery);
    Server server = serverDiscoverySelector.pick();
    EasyMock.verify(discovery);
    Assert.assertEquals(PORT, server.getPort());
    Assert.assertEquals(ADDRESS, server.getAddress());
    Assert.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assert.assertTrue(server.getHost().contains(ADDRESS));
    Assert.assertEquals("http", server.getScheme());
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assert.assertEquals(PORT, uri.getPort());
    Assert.assertEquals(ADDRESS, uri.getHost());
    Assert.assertEquals("http", uri.getScheme());
  }

  @Test
  public void testPickIPv6() throws Exception
  {
    final String address = "2001:0db8:0000:0000:0000:ff00:0042:8329";
    EasyMock.expect(discovery.getLeaderForType("coordinator"))
            .andReturn(new DruidServerMetadata("coor", address + ":" + PORT, 0, "coordinator", "t1", 0, "service",
                                               address,
                                               PORT))
            .once();
    EasyMock.replay(discovery);
    Server server = serverDiscoverySelector.pick();
    Assert.assertEquals(PORT, server.getPort());
    Assert.assertEquals(address, server.getAddress());
    Assert.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assert.assertTrue(server.getHost().contains(address));
    Assert.assertEquals("http", server.getScheme());
    EasyMock.verify(discovery);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assert.assertEquals(PORT, uri.getPort());
    Assert.assertEquals(String.format("[%s]", address), uri.getHost());
    Assert.assertEquals("http", uri.getScheme());
  }

  @Test
  public void testPickIPv6Bracket() throws Exception
  {
    final String address = "[2001:0db8:0000:0000:0000:ff00:0042:8329]";
    EasyMock.expect(discovery.getLeaderForType("coordinator"))
            .andReturn(new DruidServerMetadata("coor", address + ":" + PORT, 0, "coordinator", "t1", 0, "service",
                                               address,
                                               PORT))
            .once();
    EasyMock.replay(discovery);
    Server server = serverDiscoverySelector.pick();
    Assert.assertEquals(PORT, server.getPort());
    Assert.assertEquals(address, server.getAddress());
    Assert.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assert.assertTrue(server.getHost().contains(address));
    Assert.assertEquals("http", server.getScheme());
    EasyMock.verify(discovery);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assert.assertEquals(PORT, uri.getPort());
    Assert.assertEquals(address, uri.getHost());
    Assert.assertEquals("http", uri.getScheme());
  }

  @Test
  public void testPickWithNullInstance() throws Exception
  {
    EasyMock.expect(discovery.getLeaderForType("coordinator"))
            .andReturn(null)
            .once();
    EasyMock.replay(discovery);
    Server server = serverDiscoverySelector.pick();
    Assert.assertNull(server);
    EasyMock.verify(discovery);
  }

  @Test
  public void testPickWithException() throws Exception
  {
    EasyMock.expect(discovery.getLeaderForType("coordinator")).andThrow(new ISE("ise")).once();
    EasyMock.replay(discovery);
    Server server = serverDiscoverySelector.pick();
    Assert.assertNull(server);
    EasyMock.verify(discovery);
  }

  @Test
  public void testName() throws Exception
  {
    System.out.println(HostAndPort.fromString("[2001:0db8:0000:0000:0000:ff00:0042:8329]:80"));
  }

}
