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

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Assert;
import org.junit.Test;

public class PlannerConfigTest
{
  @Test
  public void testPlannerConfigDefaults()
  {
    PlannerConfig config = new PlannerConfig();
    Assert.assertFalse(config.isUseLexicographicTopN());
    Assert.assertTrue(config.isUseApproximateTopN());
    Assert.assertTrue(config.isUseApproximateCountDistinct());
  }

  @Test
  public void testPlannerConfigBuilder()
  {
    PlannerConfig config = PlannerConfig.builder()
                                        .useLexicographicTopN(true)
                                        .build();
    Assert.assertTrue(config.isUseLexicographicTopN());
    Assert.assertTrue(config.isUseApproximateTopN());
  }

  @Test
  public void testPlannerConfigBuilderBothTopNFlagsDisabled()
  {
    PlannerConfig config = PlannerConfig.builder()
                                        .useLexicographicTopN(false)
                                        .useApproximateTopN(false)
                                        .build();
    Assert.assertFalse(config.isUseLexicographicTopN());
    Assert.assertFalse(config.isUseApproximateTopN());
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
