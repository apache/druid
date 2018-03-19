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

import io.druid.client.selector.Server;
import io.druid.java.util.common.Intervals;
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

    EasyMock.expect(brokerSelector.select(EasyMock.anyObject(Query.class))).andReturn(
        Pair.of("service", server)
    );
    EasyMock.replay(brokerSelector);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(brokerSelector);
  }

  @Test
  public void testFindServer()
  {
    QueryHostFinder queryRunner = new QueryHostFinder(
        brokerSelector,
        new RendezvousHashAvaticaConnectionBalancer()
    );

    Server server = queryRunner.findServer(
        new TimeBoundaryQuery(
            new TableDataSource("test"),
            new MultipleIntervalSegmentSpec(Arrays.<Interval>asList(Intervals.of("2011-08-31/2011-09-01"))),
            null,
            null,
            null
        )
    );

    Assert.assertEquals("foo", server.getHost());
  }
}
