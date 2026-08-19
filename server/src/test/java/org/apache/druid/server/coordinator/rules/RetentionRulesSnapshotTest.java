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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RetentionRulesSnapshotTest
{
  private static final Rule DATASOURCE_RULE = new IntervalLoadRule(
      Intervals.of("2012-01-01/2012-01-02"),
      Map.of(DruidServer.DEFAULT_TIER, 2),
      null
  );
  private static final Rule DEFAULT_RULE = new ForeverLoadRule(Map.of(DruidServer.DEFAULT_TIER, 1), null);

  @Test
  void testDatasourceRulesArePrependedToClusterDefaults()
  {
    final RetentionRulesSnapshot rules = new RetentionRulesSnapshot(
        Map.of(TestDataSource.WIKI, List.of(DATASOURCE_RULE)),
        List.of(DEFAULT_RULE)
    );

    Assertions.assertEquals(
        List.of(DATASOURCE_RULE, DEFAULT_RULE),
        rules.getRulesWithDefault(TestDataSource.WIKI)
    );
  }

  @Test
  void testDatasourceWithNoRulesResolvesToClusterDefaults()
  {
    final RetentionRulesSnapshot rules = new RetentionRulesSnapshot(
        Map.of(TestDataSource.WIKI, List.of(DATASOURCE_RULE)),
        List.of(DEFAULT_RULE)
    );

    Assertions.assertEquals(List.of(DEFAULT_RULE), rules.getRulesWithDefault(TestDataSource.KOALA));
  }

  @Test
  void testSnapshotIsUnaffectedByLaterChangesToSourceMap()
  {
    final Map<String, List<Rule>> source = new HashMap<>();
    source.put(TestDataSource.WIKI, List.of(DATASOURCE_RULE));

    final RetentionRulesSnapshot rules = new RetentionRulesSnapshot(source, List.of(DEFAULT_RULE));

    source.put(TestDataSource.WIKI, List.of(DEFAULT_RULE));
    source.put(TestDataSource.KOALA, List.of(DATASOURCE_RULE));

    Assertions.assertEquals(
        List.of(DATASOURCE_RULE, DEFAULT_RULE),
        rules.getRulesWithDefault(TestDataSource.WIKI)
    );
    Assertions.assertEquals(List.of(DEFAULT_RULE), rules.getRulesWithDefault(TestDataSource.KOALA));
  }

  @Test
  void testSnapshotIsUnaffectedByLaterChangesToSourceRuleList()
  {
    final List<Rule> wikiRules = new ArrayList<>(List.of(DATASOURCE_RULE));
    final RetentionRulesSnapshot rules = new RetentionRulesSnapshot(
        Map.of(TestDataSource.WIKI, wikiRules),
        List.of(DEFAULT_RULE)
    );

    wikiRules.clear();

    Assertions.assertEquals(List.of(DATASOURCE_RULE), rules.getRules(TestDataSource.WIKI));
    Assertions.assertEquals(
        List.of(DATASOURCE_RULE, DEFAULT_RULE),
        rules.getRulesWithDefault(TestDataSource.WIKI)
    );
  }

  @Test
  void testEmptySnapshotHasNoRulesForAnyDatasource()
  {
    Assertions.assertEquals(List.of(), RetentionRulesSnapshot.empty().getRulesWithDefault(TestDataSource.WIKI));
  }
}
