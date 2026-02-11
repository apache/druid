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

package org.apache.druid.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.Druids;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.junit.Assert;
import org.junit.Test;
import java.util.Map;
import java.util.Set;

public class QueryBlocklistRuleTest
{
  @Test
  public void testMatchAllCriteria_rejectsNullCriteria()
  {
    // Rule with all null criteria would block ALL queries - this should be rejected
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryBlocklistRule("match-all", null, null, null)
    );
  }

  @Test
  public void testMatchAllCriteria_rejectsEmptyCollections()
  {
    // Rule with all empty collections should also be rejected (same as null)
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryBlocklistRule("match-all", ImmutableSet.of(), ImmutableSet.of(), ImmutableMap.of())
    );
  }

  @Test
  public void testMatchByDataSource()
  {
    Set<String> dataSources = ImmutableSet.of("sensitive_data", "pii_table");
    QueryBlocklistRule rule = new QueryBlocklistRule("block-sensitive", dataSources, null, null);

    // Should match when datasource is in the list
    TimeseriesQuery matchingQuery = Druids.newTimeseriesQueryBuilder()
                                   .dataSource("sensitive_data")
                                   .intervals("2020-01-01/2020-01-02")
                                   .build();
    Assert.assertTrue(rule.matches(matchingQuery));

    // Should not match when datasource is not in the list
    TimeseriesQuery nonMatchingQuery = Druids.newTimeseriesQueryBuilder()
                                      .dataSource("safe_data")
                                      .intervals("2020-01-01/2020-01-02")
                                      .build();
    Assert.assertFalse(rule.matches(nonMatchingQuery));
  }

  @Test
  public void testMatchByContext()
  {
    Map<String, String> contextMatches = ImmutableMap.of("priority", "0", "application", "rogue-app");
    QueryBlocklistRule rule = new QueryBlocklistRule("block-rogue-app", null, null, contextMatches);

    // Should match when all context values match
    TimeseriesQuery matchingQuery = Druids.newTimeseriesQueryBuilder()
                                   .dataSource("test")
                                   .intervals("2020-01-01/2020-01-02")
                                   .context(ImmutableMap.of("priority", "0", "application", "rogue-app"))
                                   .build();
    Assert.assertTrue(rule.matches(matchingQuery));

    // Should not match when context values don't match
    TimeseriesQuery nonMatchingQuery = Druids.newTimeseriesQueryBuilder()
                                      .dataSource("test")
                                      .intervals("2020-01-01/2020-01-02")
                                      .context(ImmutableMap.of("priority", "1", "application", "rogue-app"))
                                      .build();
    Assert.assertFalse(rule.matches(nonMatchingQuery));

    // Should not match when context is missing
    TimeseriesQuery noContextQuery = Druids.newTimeseriesQueryBuilder()
                                    .dataSource("test")
                                    .intervals("2020-01-01/2020-01-02")
                                    .build();
    Assert.assertFalse(rule.matches(noContextQuery));
  }

  @Test
  public void testMatchByMultipleCriteria()
  {
    // Rule with multiple criteria - all must match (AND logic)
    Set<String> dataSources = ImmutableSet.of("large_table");
    Map<String, String> contextMatches = ImmutableMap.of("priority", "0");
    QueryBlocklistRule rule = new QueryBlocklistRule(
        "block-low-priority-large-table",
        dataSources,
        null,
        contextMatches
    );

    // Should match when both datasource AND context match
    TimeseriesQuery matchingQuery = Druids.newTimeseriesQueryBuilder()
                                   .dataSource("large_table")
                                   .intervals("2020-01-01/2020-01-02")
                                   .context(ImmutableMap.of("priority", "0"))
                                   .build();
    Assert.assertTrue(rule.matches(matchingQuery));

    // Should not match when only datasource matches
    TimeseriesQuery onlyDataSourceMatches = Druids.newTimeseriesQueryBuilder()
                                           .dataSource("large_table")
                                           .intervals("2020-01-01/2020-01-02")
                                           .context(ImmutableMap.of("priority", "1"))
                                           .build();
    Assert.assertFalse(rule.matches(onlyDataSourceMatches));

    // Should not match when only context matches
    TimeseriesQuery onlyContextMatches = Druids.newTimeseriesQueryBuilder()
                                        .dataSource("small_table")
                                        .intervals("2020-01-01/2020-01-02")
                                        .context(ImmutableMap.of("priority", "0"))
                                        .build();
    Assert.assertFalse(rule.matches(onlyContextMatches));
  }

  @Test
  public void testWildcardBehavior_nullQueryTypes()
  {
    QueryBlocklistRule rule = new QueryBlocklistRule(
        "block-datasource-all-types",
        ImmutableSet.of("blocked_ds"),
        null,  // null means match all query types
        null
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                           .dataSource("blocked_ds")
                           .intervals("2020-01-01/2020-01-02")
                           .build();

    Assert.assertTrue(rule.matches(query));
  }

  @Test
  public void testRuleNameValidation_null()
  {
    // Rule name cannot be null
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryBlocklistRule(null, ImmutableSet.of("ds"), null, null)
    );
  }

  @Test
  public void testRuleNameValidation_empty()
  {
    // Rule name cannot be empty
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> new QueryBlocklistRule("", ImmutableSet.of("ds"), null, null)
    );
  }
}
