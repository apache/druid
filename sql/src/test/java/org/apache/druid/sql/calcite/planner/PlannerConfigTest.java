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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class PlannerConfigTest
{
  @Test
  public void testPlannerConfigDefaults()
  {
    PlannerConfig config = new PlannerConfig();
    Assertions.assertFalse(config.isUseLexicographicTopN());
    Assertions.assertTrue(config.isUseApproximateTopN());
    Assertions.assertTrue(config.isUseApproximateCountDistinct());
  }

  @Test
  public void testMaxPlanningTimeMsDisabledByDefault()
  {
    PlannerConfig config = new PlannerConfig();
    Assertions.assertEquals(PlannerConfig.PLANNING_TIME_NOT_LIMITED, config.getMaxPlanningTimeMs());
    Assertions.assertFalse(config.isPlanningTimeLimited());
  }

  @Test
  public void testMaxPlanningTimeMsBuilder()
  {
    PlannerConfig config = PlannerConfig.builder()
                                        .maxPlanningTimeMs(5000)
                                        .build();
    Assertions.assertEquals(5000, config.getMaxPlanningTimeMs());
    Assertions.assertTrue(config.isPlanningTimeLimited());
  }

  @Test
  public void testMaxPlanningTimeMsQueryContextOverride()
  {
    PlannerConfig base = PlannerConfig.builder().maxPlanningTimeMs(10_000).build();
    PlannerConfig overridden = base.withOverrides(
        ImmutableMap.of(PlannerConfig.CTX_KEY_MAX_PLANNING_TIME_MS, 2500)
    );
    Assertions.assertEquals(2500, overridden.getMaxPlanningTimeMs());
    // The base config is untouched.
    Assertions.assertEquals(10_000, base.getMaxPlanningTimeMs());
  }

  @Test
  public void testMaxPlanningTimeMsInheritedWhenNotOverridden()
  {
    PlannerConfig base = PlannerConfig.builder().maxPlanningTimeMs(10_000).build();
    PlannerConfig overridden = base.withOverrides(ImmutableMap.of("someOtherKey", "someValue"));
    Assertions.assertEquals(10_000, overridden.getMaxPlanningTimeMs());
  }

  @Test
  public void testMaxPlanningTimeMsRoundTripsThroughQueryContext()
  {
    // A non-default maxPlanningTimeMs must be emitted by getNonDefaultAsQueryContext() so the defensive
    // config <-> context round-trip check inside that method passes.
    PlannerConfig config = PlannerConfig.builder().maxPlanningTimeMs(5000).build();
    Map<String, Object> asContext = config.getNonDefaultAsQueryContext();
    Assertions.assertEquals(5000L, ((Number) asContext.get(PlannerConfig.CTX_KEY_MAX_PLANNING_TIME_MS)).longValue());
    Assertions.assertEquals(config, PlannerConfig.builder().withOverrides(asContext).build());
  }

  @Test
  public void testPlannerConfigBuilder()
  {
    PlannerConfig config = PlannerConfig.builder()
                                        .useLexicographicTopN(true)
                                        .build();
    Assertions.assertTrue(config.isUseLexicographicTopN());
    Assertions.assertTrue(config.isUseApproximateTopN());
  }

  @Test
  public void testPlannerConfigBuilderBothTopNFlagsDisabled()
  {
    PlannerConfig config = PlannerConfig.builder()
                                        .useLexicographicTopN(false)
                                        .useApproximateTopN(false)
                                        .build();
    Assertions.assertFalse(config.isUseLexicographicTopN());
    Assertions.assertFalse(config.isUseApproximateTopN());
  }

  @Test
  public void testPlannerConfigEqualsAndHashCode()
  {
    EqualsVerifier.forClass(PlannerConfig.class)
                  .usingGetClass()
                  .suppress(Warning.NONFINAL_FIELDS)
                  .verify();
  }
}
