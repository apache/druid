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

package org.apache.druid.server.scheduling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryLaningStrategy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class CompositeQueryLaningStrategyTest
{
  private static final int TOTAL_LIMIT = 1000;
  private static final String STRATEGY_ONE_LANE_ONE = "s1-l1";
  private static final String STRATEGY_ONE_LANE_TWO = "s1-l2";
  private static final String STRATEGY_ONE_LANE_THREE = "s1-l3";
  private static final String STRATEGY_TWO_LANE_ONE = "s2-l1";
  private static final String STRATEGY_TWO_LANE_TWO = "s2-l2";
  private static final List<String> ALL_LANES = ImmutableList.of(
      STRATEGY_ONE_LANE_ONE,
      STRATEGY_ONE_LANE_TWO,
      STRATEGY_ONE_LANE_THREE,
      STRATEGY_TWO_LANE_ONE,
      STRATEGY_TWO_LANE_TWO
  );
  private static final Object2IntMap<String> STRATEGY_ONE_LIMITS = new Object2IntArrayMap<>();
  private static final Object2IntMap<String> STRATEGY_TWO_LIMITS = new Object2IntArrayMap<>();
  private static final Optional<String> STRATEGY_ONE_COMPUTED_LANE = Optional.of("STRATEGY_ONE_COMPUTED_LANE");
  private static final Optional<String> STRATEGY_TWO_COMPUTED_LANE = Optional.of("STRATEGY_TWO_COMPUTED_LANE");

  static {
    STRATEGY_ONE_LIMITS.put(STRATEGY_ONE_LANE_ONE, 2);
    STRATEGY_ONE_LIMITS.put(STRATEGY_ONE_LANE_TWO, 3);
    STRATEGY_ONE_LIMITS.put(STRATEGY_ONE_LANE_THREE, 4);
    STRATEGY_TWO_LIMITS.put(STRATEGY_TWO_LANE_ONE, 30);
    STRATEGY_TWO_LIMITS.put(STRATEGY_TWO_LANE_TWO, 40);
  }

  @Mock
  private CompositeQueryLaningStrategy subCompositeStrategy;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private QueryLaningStrategy strategyOne;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private QueryLaningStrategy strategyTwo;
  @Mock
  private Query<String> query;
  @Mock
  private Set<SegmentServerSelector> segments;

  private QueryPlus<String> queryPlus;
  private CompositeQueryLaningStrategy target;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    queryPlus = QueryPlus.wrap(query);
    Mockito.doReturn(STRATEGY_ONE_COMPUTED_LANE).when(strategyOne).computeLane(queryPlus, segments);
    Mockito.doReturn(STRATEGY_TWO_COMPUTED_LANE).when(strategyTwo).computeLane(queryPlus, segments);
    mockLimits();
    target = new CompositeQueryLaningStrategy(
        ImmutableMap.of(
            "one", strategyOne,
            "two", strategyTwo
        )
    );
  }

  @Test
  public void initStrategiesMustBeSet()
  {
    expectedException.expect(NullPointerException.class);
    expectedException.expectMessage("strategies must be set.");
    target = new CompositeQueryLaningStrategy(null);
  }

  @Test
  public void initMinimumNumberOfExpectedStrategies()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("strategies must define at least one strategy.");
    target = new CompositeQueryLaningStrategy(Collections.emptyMap());
  }

  @Test
  public void initNestedCompositeQueryLaningStrategiesAreNotAllowed()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("strategies can not contain a composite strategy.");
    target = new CompositeQueryLaningStrategy(ImmutableMap.of("composite", subCompositeStrategy));
  }

  @Test
  public void initNestedCompositeQueryLaningStrategiesWithOtherValidStrategiesAreNotAllowed()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("strategies can not contain a composite strategy.");
    target = new CompositeQueryLaningStrategy(
        ImmutableMap.of(
            "one", strategyOne,
            "composite", subCompositeStrategy,
            "two", strategyTwo
        )
    );
  }

  @Test
  public void getLaneLimitsShouldIncludeAllLaneLimits()
  {
    Object2IntMap<String> laneLimits = target.getLaneLimits(TOTAL_LIMIT);
    Assert.assertEquals(5, laneLimits.size());
    Assert.assertTrue(laneLimits.keySet().containsAll(ALL_LANES));
    Assert.assertEquals(79, laneLimits.values().stream().reduce(0, Integer::sum).intValue());
  }

  @Test
  public void computeLaneWithoutLateStrategyShouldReturnEmpty()
  {
    Optional<String> lane = target.computeLane(queryPlus, segments);
    Assert.assertFalse(lane.isPresent());
  }

  @Test
  public void computeLaneWithNonExistentLateStrategyShouldReturnEmpty()
  {
    mockQueryStrategy("unknown");
    Optional<String> lane = target.computeLane(queryPlus, segments);
    Assert.assertFalse(lane.isPresent());
  }

  @Test
  public void computeLaneWithLateStrategyShouldReturnStrategyFromSelectedStrategy()
  {
    mockQueryStrategy("one");
    Optional<String> lane = target.computeLane(queryPlus, segments);
    Assert.assertEquals(STRATEGY_ONE_COMPUTED_LANE, lane);
    mockQueryStrategy("two");
    lane = target.computeLane(queryPlus, segments);
    Assert.assertEquals(STRATEGY_TWO_COMPUTED_LANE, lane);
  }

  private void mockQueryStrategy(String strategy)
  {
    Mockito.doReturn(strategy).when(query).getContextValue(QueryContexts.COMPOSITE_LANE_STRATEGY_KEY);
  }

  private void mockLimits()
  {
    Mockito.doReturn(STRATEGY_ONE_LIMITS).when(strategyOne).getLaneLimits(TOTAL_LIMIT);
    Mockito.doReturn(STRATEGY_TWO_LIMITS).when(strategyTwo).getLaneLimits(TOTAL_LIMIT);
  }
}
