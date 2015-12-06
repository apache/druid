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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.metamx.http.client.HttpClient;
import io.druid.client.DruidServer;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.query.Druids;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 */
public class TieredBrokerHostSelectorTest
{
  private ServerDiscoveryFactory factory;
  private ServerDiscoverySelector selector;
  private TieredBrokerHostSelector brokerSelector;

  @Before
  public void setUp() throws Exception
  {
    factory = EasyMock.createMock(ServerDiscoveryFactory.class);
    selector = EasyMock.createMock(ServerDiscoverySelector.class);

    brokerSelector = new TieredBrokerHostSelector(
        new TestRuleManager(null, null, null, null),
        new TieredBrokerConfig()
        {
          @Override
          public LinkedHashMap<String, String> getTierToBrokerMap()
          {
            return new LinkedHashMap<String, String>(
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
        },
        factory,
        Arrays.asList(new TimeBoundaryTieredBrokerSelectorStrategy(), new PriorityTieredBrokerSelectorStrategy(0, 1))
    );
    EasyMock.expect(factory.createSelector(EasyMock.<String>anyObject())).andReturn(selector).atLeastOnce();
    EasyMock.replay(factory);

    selector.start();
    EasyMock.expectLastCall().atLeastOnce();
    selector.stop();
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(selector);

    brokerSelector.start();

  }

  @After
  public void tearDown() throws Exception
  {
    brokerSelector.stop();

    EasyMock.verify(selector);
    EasyMock.verify(factory);
  }

  @Test
  public void testBasicSelect() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
              .intervals(Arrays.<Interval>asList(new Interval("2011-08-31/2011-09-01")))
              .build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }


  @Test
  public void testBasicSelect2() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
              .intervals(Arrays.<Interval>asList(new Interval("2013-08-31/2013-09-01")))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testSelectMatchesNothing() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
              .intervals(Arrays.<Interval>asList(new Interval("2010-08-31/2010-09-01")))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testSelectMultiInterval() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.<Interval>asList(
                          new Interval("2013-08-31/2013-09-01"),
                          new Interval("2012-08-31/2012-09-01"),
                          new Interval("2011-08-31/2011-09-01")
                      )
                  )
              ).build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testSelectMultiInterval2() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.<Interval>asList(
                          new Interval("2011-08-31/2011-09-01"),
                          new Interval("2012-08-31/2012-09-01"),
                          new Interval("2013-08-31/2013-09-01")
                      )
                  )
              ).build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testPrioritySelect() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.<Interval>asList(
                          new Interval("2011-08-31/2011-09-01"),
                          new Interval("2012-08-31/2012-09-01"),
                          new Interval("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.<String, Object>of("priority", -1))
              .build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testPrioritySelect2() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.<Interval>asList(
                          new Interval("2011-08-31/2011-09-01"),
                          new Interval("2012-08-31/2012-09-01"),
                          new Interval("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.<String, Object>of("priority", 5))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  private static class TestRuleManager extends CoordinatorRuleManager
  {
    public TestRuleManager(
        @Global HttpClient httpClient,
        @Json ObjectMapper jsonMapper,
        Supplier<TieredBrokerConfig> config,
        ServerDiscoverySelector selector
    )
    {
      super(httpClient, jsonMapper, config, selector);
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public List<Rule> getRulesWithDefault(String dataSource)
    {
      return Arrays.<Rule>asList(
          new IntervalLoadRule(new Interval("2013/2014"), ImmutableMap.<String, Integer>of("hot", 1)),
          new IntervalLoadRule(new Interval("2012/2013"), ImmutableMap.<String, Integer>of("medium", 1)),
          new IntervalLoadRule(
              new Interval("2011/2012"),
              ImmutableMap.<String, Integer>of(DruidServer.DEFAULT_TIER, 1)
          )
      );
    }
  }
}
