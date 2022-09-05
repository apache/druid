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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.server.coordinator.rules.ForeverDropRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.PeriodLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CoordinatorRuleManagerTest
{
  private static final String DATASOURCE1 = "datasource1";
  private static final String DATASOURCE2 = "datasource2";
  private static final List<Rule> DEFAULT_RULES = ImmutableList.of(
      new ForeverLoadRule(ImmutableMap.of("__default", 2))
  );

  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final TieredBrokerConfig tieredBrokerConfig = new TieredBrokerConfig();

  @Test
  public void testAddingToRulesMapThrowingError()
  {
    final CoordinatorRuleManager manager = new CoordinatorRuleManager(
        objectMapper,
        () -> tieredBrokerConfig,
        mockClient()
    );
    final Map<String, List<Rule>> rules = manager.getRules();
    expectedException.expect(UnsupportedOperationException.class);
    rules.put("testKey", Collections.emptyList());
  }

  @Test
  public void testAddingToRulesListThrowingError()
  {
    final CoordinatorRuleManager manager = new CoordinatorRuleManager(
        objectMapper,
        () -> tieredBrokerConfig,
        mockClient()
    );
    manager.poll();
    final Map<String, List<Rule>> rules = manager.getRules();
    expectedException.expect(UnsupportedOperationException.class);
    rules.get(DATASOURCE1).add(new ForeverDropRule());
  }

  @Test
  public void testGetRulesWithUnknownDatasourceReturningDefaultRule()
  {
    final CoordinatorRuleManager manager = new CoordinatorRuleManager(
        objectMapper,
        () -> tieredBrokerConfig,
        mockClient()
    );
    manager.poll();
    final List<Rule> rules = manager.getRulesWithDefault("unknown");
    Assert.assertEquals(DEFAULT_RULES, rules);
  }

  @Test
  public void testGetRulesWithKnownDatasourceReturningAllRulesWithDefaultRule()
  {
    final CoordinatorRuleManager manager = new CoordinatorRuleManager(
        objectMapper,
        () -> tieredBrokerConfig,
        mockClient()
    );
    manager.poll();
    final List<Rule> rules = manager.getRulesWithDefault(DATASOURCE2);
    final List<Rule> expectedRules = new ArrayList<>();
    expectedRules.add(new ForeverLoadRule(null));
    expectedRules.add(new IntervalDropRule(Intervals.of("2020-01-01/2020-01-02")));
    expectedRules.addAll(DEFAULT_RULES);
    Assert.assertEquals(expectedRules, rules);
  }

  private DruidLeaderClient mockClient()
  {
    final Map<String, List<Rule>> rules = ImmutableMap.of(
        DATASOURCE1,
        ImmutableList.of(new ForeverLoadRule(null)),
        DATASOURCE2,
        ImmutableList.of(new ForeverLoadRule(null), new IntervalDropRule(Intervals.of("2020-01-01/2020-01-02"))),
        "datasource3",
        ImmutableList.of(
            new PeriodLoadRule(new Period("P1M"), true, null),
            new ForeverDropRule()
        ),
        TieredBrokerConfig.DEFAULT_RULE_NAME,
        ImmutableList.of(new ForeverLoadRule(ImmutableMap.of("__default", 2)))
    );
    final StringFullResponseHolder holder = EasyMock.niceMock(StringFullResponseHolder.class);
    EasyMock.expect(holder.getStatus())
            .andReturn(HttpResponseStatus.OK);
    try {
      EasyMock.expect(holder.getContent())
              .andReturn(objectMapper.writeValueAsString(rules));
      final DruidLeaderClient client = EasyMock.niceMock(DruidLeaderClient.class);
      EasyMock.expect(client.go(EasyMock.anyObject()))
              .andReturn(holder);
      EasyMock.replay(holder, client);
      return client;
    }
    catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
