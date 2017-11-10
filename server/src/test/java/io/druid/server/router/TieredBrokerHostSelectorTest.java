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
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.client.DruidServer;
import io.druid.client.selector.Server;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.query.Druids;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.server.DruidNode;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.Rule;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/**
 */
public class TieredBrokerHostSelectorTest
{
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private DruidNodeDiscovery druidNodeDiscovery;
  private TieredBrokerHostSelector brokerSelector;

  private DiscoveryDruidNode node1;
  private DiscoveryDruidNode node2;
  private DiscoveryDruidNode node3;

  @Before
  public void setUp() throws Exception
  {
    druidNodeDiscoveryProvider = EasyMock.createStrictMock(DruidNodeDiscoveryProvider.class);

    node1 = new DiscoveryDruidNode(
        new DruidNode("hotBroker", "hotHost", 8080, null, true, false),
        DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
        ImmutableMap.of()
    );

    node2 = new DiscoveryDruidNode(
        new DruidNode("coldBroker", "coldHost1", 8080, null, true, false),
        DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
        ImmutableMap.of()
    );

    node3 = new DiscoveryDruidNode(
        new DruidNode("coldBroker", "coldHost2", 8080, null, true, false),
        DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
        ImmutableMap.of()
    );

    druidNodeDiscovery = new DruidNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryDruidNode> getAllNodes()
      {
        return ImmutableSet.of(node1, node2, node3);
      }

      @Override
      public void registerListener(Listener listener)
      {
        listener.nodesAdded(ImmutableList.of(node1, node2, node3));
      }
    };

    EasyMock.expect(druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_BROKER))
            .andReturn(druidNodeDiscovery);;

    EasyMock.replay(druidNodeDiscoveryProvider);

    brokerSelector = new TieredBrokerHostSelector(
        new TestRuleManager(null, null),
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
        druidNodeDiscoveryProvider,
        Arrays.asList(new TimeBoundaryTieredBrokerSelectorStrategy(), new PriorityTieredBrokerSelectorStrategy(0, 1))
    );

    brokerSelector.start();
  }

  @After
  public void tearDown() throws Exception
  {
    brokerSelector.stop();

    EasyMock.verify(druidNodeDiscoveryProvider);
  }

  @Test
  public void testBasicSelect() throws Exception
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity("all")
                                  .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
                                  .intervals(Arrays.<Interval>asList(Intervals.of("2011-08-31/2011-09-01")))
                                  .build();

    Pair<String, Server> p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost1:8080", p.rhs.getHost());

    p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost2:8080", p.rhs.getHost());

    p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost1:8080", p.rhs.getHost());
  }


  @Test
  public void testBasicSelect2() throws Exception
  {
    Pair<String, Server> p = brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
              .intervals(Arrays.<Interval>asList(Intervals.of("2013-08-31/2013-09-01")))
              .build()
    );

    Assert.assertEquals("hotBroker", p.lhs);
    Assert.assertEquals("hotHost:8080", p.rhs.getHost());
  }

  @Test
  public void testSelectMatchesNothing() throws Exception
  {
    String brokerName = (String) brokerSelector.select(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("rows")))
              .intervals(Arrays.<Interval>asList(Intervals.of("2010-08-31/2010-09-01")))
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
                          Intervals.of("2013-08-31/2013-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2011-08-31/2011-09-01")
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
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
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
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.<String, Object>of("priority", -1))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
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
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.<String, Object>of("priority", 5))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testGetAllBrokers()
  {
    Assert.assertEquals(
        ImmutableMap.of(
            "mediumBroker", ImmutableList.of(),
            "coldBroker", ImmutableList.of("coldHost1:8080", "coldHost2:8080"),
            "hotBroker", ImmutableList.of("hotHost:8080")
        ),
        Maps.transformValues(
            brokerSelector.getAllBrokers(),
            new Function<List<Server>, List<String>>()
            {
              @Override
              public List<String> apply(@Nullable List<Server> servers)
              {
                return Lists.transform(servers, server -> server.getHost());
              }
            }
        )
    );
  }

  private static class TestRuleManager extends CoordinatorRuleManager
  {
    public TestRuleManager(
        @Json ObjectMapper jsonMapper,
        Supplier<TieredBrokerConfig> config
    )
    {
      super(jsonMapper, config, null);
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
          new IntervalLoadRule(Intervals.of("2013/2014"), ImmutableMap.<String, Integer>of("hot", 1)),
          new IntervalLoadRule(Intervals.of("2012/2013"), ImmutableMap.<String, Integer>of("medium", 1)),
          new IntervalLoadRule(
              Intervals.of("2011/2012"),
              ImmutableMap.<String, Integer>of(DruidServer.DEFAULT_TIER, 1)
          )
      );
    }
  }
}
