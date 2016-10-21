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

package io.druid.server.router;

import com.google.common.collect.ImmutableMap;

import io.druid.client.DruidServer;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.java.util.common.Pair;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 */
public class QueryHostFinderTest
{
  private ServerDiscoverySelector selector;
  private TieredBrokerHostSelector brokerSelector;
  private TieredBrokerConfig config;
  private Server server;

  @Before
  public void setUp() throws Exception
  {
    selector = EasyMock.createMock(ServerDiscoverySelector.class);
    brokerSelector = EasyMock.createMock(TieredBrokerHostSelector.class);

    config = new TieredBrokerConfig()
    {
      @Override
      public LinkedHashMap<String, String> getTierToBrokerMap()
      {
        return new LinkedHashMap<>(
            ImmutableMap.<String, String>of(
                "hot", "hotBroker",
                "medium", "mediumBroker",
                DruidServer.DEFAULT_TIER, "coldBroker"
            )
        );
      }

      @Override
      public String getDefaultBrokerServiceName()
      {
        return "hotBroker";
      }
    };

    server = new Server()
    {
      @Override
      public String getScheme()
      {
        return null;
      }

      @Override
      public String getHost()
      {
        return "foo";
      }

      @Override
      public String getAddress()
      {
        return null;
      }

      @Override
      public int getPort()
      {
        return 0;
      }
    };
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(brokerSelector);
    EasyMock.verify(selector);
  }

  @Test
  public void testFindServer() throws Exception
  {
    EasyMock.expect(brokerSelector.select(EasyMock.<Query>anyObject())).andReturn(new Pair("hotBroker", selector));
    EasyMock.replay(brokerSelector);

    EasyMock.expect(selector.pick()).andReturn(server).once();
    EasyMock.replay(selector);

    QueryHostFinder queryRunner = new QueryHostFinder(
        brokerSelector
    );

    Server server = queryRunner.findServer(
        new TimeBoundaryQuery(
            new TableDataSource("test"),
            new MultipleIntervalSegmentSpec(Arrays.<Interval>asList(new Interval("2011-08-31/2011-09-01"))),
            null,
            null,
            null
        )
    );

    Assert.assertEquals("foo", server.getHost());
  }
}
