/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.router;

import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 */
public class TierAwareQueryRunnerTest
{
  private ServerDiscoveryFactory factory;
  private ServerDiscoverySelector selector;
  private TierConfig config;
  private Server server;

  @Before
  public void setUp() throws Exception
  {
    factory = EasyMock.createMock(ServerDiscoveryFactory.class);
    selector = EasyMock.createMock(ServerDiscoverySelector.class);

    config = new TierConfig()
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
    EasyMock.verify(selector);
    EasyMock.verify(factory);
  }

  @Test
  public void testFindServer() throws Exception
  {
    selector.start();
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(selector.pick()).andReturn(server).once();
    EasyMock.replay(selector);

    EasyMock.expect(factory.createSelector(EasyMock.<String>anyObject())).andReturn(selector).atLeastOnce();
    EasyMock.replay(factory);


    TierAwareQueryRunner queryRunner = new TierAwareQueryRunner(
        null,
        null,
        null,
        new BrokerSelector(new CoordinatorRuleManager(null, null, null, null), config),
        config,
        factory
    );

    Server server = queryRunner.findServer(
        new TimeBoundaryQuery(
            new TableDataSource("test"),
            new MultipleIntervalSegmentSpec(Arrays.<Interval>asList(new Interval("2011-08-31/2011-09-01"))),
            null
        )
    );

    Assert.assertEquals("foo", server.getHost());
  }
}
