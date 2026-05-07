/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.server.router;

import org.apache.druid.client.selector.Server;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

/**
 */
public class QueryHostFinderTest
{
  private TieredBrokerHostSelector brokerSelector;
  private Server server;

  @Before
  public void setUp()
  {
    brokerSelector = EasyMock.createMock(TieredBrokerHostSelector.class);

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
  public void tearDown()
  {
    EasyMock.verify(brokerSelector);
  }

  @Test
  public void testFindServer()
  {
    EasyMock.expect(brokerSelector.select(EasyMock.anyObject(Query.class))).andReturn(
        Pair.of("service", server)
    );
    EasyMock.replay(brokerSelector);

    final Server selectedServer = newQueryHostFinder().findServer(newQuery());

    Assert.assertEquals("foo", selectedServer.getHost());
  }

  @Test
  public void testFindServerUsesBackupWhenVersionFilterIsDisabled()
  {
    EasyMock.expect(brokerSelector.select(EasyMock.anyObject(Query.class))).andReturn(
        Pair.of("service", server)
    );
    EasyMock.expect(brokerSelector.select(EasyMock.anyObject(Query.class))).andReturn(
        Pair.of("service", null)
    );
    EasyMock.expect(brokerSelector.isVersionFilterEnabled()).andReturn(false);
    EasyMock.replay(brokerSelector);

    final QueryHostFinder queryHostFinder = newQueryHostFinder();

    Assert.assertSame(server, queryHostFinder.findServer(newQuery()));
    Assert.assertSame(server, queryHostFinder.findServer(newQuery()));
  }

  @Test
  public void testFindServerSkipsBackupWhenVersionFilterIsEnabled()
  {
    EasyMock.expect(brokerSelector.select(EasyMock.anyObject(Query.class))).andReturn(
        Pair.of("service", server)
    );
    EasyMock.expect(brokerSelector.select(EasyMock.anyObject(Query.class))).andReturn(
        Pair.of("service", null)
    );
    EasyMock.expect(brokerSelector.isVersionFilterEnabled()).andReturn(true);
    EasyMock.replay(brokerSelector);

    final QueryHostFinder queryHostFinder = newQueryHostFinder();

    Assert.assertSame(server, queryHostFinder.findServer(newQuery()));
    Assert.assertNull(queryHostFinder.findServer(newQuery()));
  }

  private QueryHostFinder newQueryHostFinder()
  {
    return new QueryHostFinder(
        brokerSelector,
        new RendezvousHashAvaticaConnectionBalancer()
    );
  }

  private Query<?> newQuery()
  {
    return new TimeBoundaryQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-08-31/2011-09-01"))),
        null,
        null,
        null
    );
  }
}
