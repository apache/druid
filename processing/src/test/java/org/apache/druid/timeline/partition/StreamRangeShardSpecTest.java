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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StreamRangeShardSpecTest
{
  private static final String TENANT = "tenant";

  private static StreamRangeShardSpec spec(Map<String, List<String>> filters)
  {
    return new StreamRangeShardSpec(0, 1, filters);
  }

  private static RangeSet<String> points(String... values)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    for (String v : values) {
      rangeSet.add(Range.singleton(v));
    }
    return rangeSet;
  }

  private static Map<String, RangeSet<String>> domain(String dimension, String... values)
  {
    return ImmutableMap.of(dimension, points(values));
  }

  private static Map<String, RangeSet<String>> rangeFilter(String dimension, String lower, String upper)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.closed(lower, upper));
    return ImmutableMap.of(dimension, rangeSet);
  }

  /**
   * The query domain Druid builds for an {@code IS NULL} filter: a null match is encoded as the range {@code (-inf, "")}
   * (see e.g. {@code NullFilter#getDimensionRangeSet}).
   */
  private static Map<String, RangeSet<String>> nullDomain(String dimension)
  {
    final RangeSet<String> rangeSet = TreeRangeSet.create();
    rangeSet.add(Range.lessThan(""));
    return ImmutableMap.of(dimension, rangeSet);
  }

  @Test
  public void testNoFilters_alwaysTrue()
  {
    final StreamRangeShardSpec s = spec(Collections.emptyMap());
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertTrue(s.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void testSingleFilter_matchingValue_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
  }

  @Test
  public void testSingleFilter_nonMatchingValue_returnsFalse()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testSingleFilter_domainHasMultipleValues_matchIncluded_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a", "tenant_b")));
  }

  @Test
  public void testSingleFilter_domainHasMultipleValues_noMatch_returnsFalse()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b", "tenant_c")));
  }

  @Test
  public void testMultipleAllowedValues_matchOne_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testMultipleAllowedValues_noMatch_returnsFalse()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_c")));
  }

  @Test
  public void testDeclaredDimension_notInQueryDomain_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void testDeclaredDimension_queryFiltersOnOtherDim_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain("region", "us-west")));
  }

  @Test
  public void testRangeFilter_onDeclaredDimension_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    // A range predicate (e.g. TENANT BETWEEN 'a' AND 'z') cannot be pruned against declared point values.
    Assert.assertTrue(s.possibleInDomain(rangeFilter(TENANT, "a", "z")));
  }

  @Test
  public void testMultipleDimensions_allMatch_returnsTrue()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.possibleInDomain(ImmutableMap.of(
        TENANT, points("tenant_a"),
        "region", points("us-west")
    )));
  }

  @Test
  public void testMultipleDimensions_oneDimensionMismatches_returnsFalse()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertFalse(s.possibleInDomain(ImmutableMap.of(
        TENANT, points("tenant_a"),
        "region", points("eu-east")
    )));
  }

  @Test
  public void testMultipleDimensions_onlyOneDimensionInDomain()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testGetDomainDimensions_returnsFilterKeys()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.getDomainDimensions().contains(TENANT));
    Assert.assertTrue(s.getDomainDimensions().contains("region"));
    Assert.assertEquals(2, s.getDomainDimensions().size());
  }

  @Test
  public void testGetDomainDimensions_emptyFilters_returnsEmpty()
  {
    Assert.assertTrue(spec(Collections.emptyMap()).getDomainDimensions().isEmpty());
  }

  @Test
  public void testGetType()
  {
    Assert.assertEquals(ShardSpec.Type.STREAM_RANGE, spec(Collections.emptyMap()).getType());
  }

  private static ObjectMapper newMapper()
  {
    return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void testJsonSerdeRoundTrip() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final StreamRangeShardSpec original = new StreamRangeShardSpec(
        3,
        8,
        ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b"), "region", List.of("us-west"))
    );

    final StreamRangeShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), StreamRangeShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertEquals(original.getPartitionFilters(), deserialized.getPartitionFilters());
  }

  @Test
  public void testJsonSerdeContainsType() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final StreamRangeShardSpec spec = new StreamRangeShardSpec(0, 1, ImmutableMap.of(TENANT, List.of("tenant_a")));
    final String json = mapper.writeValueAsString(spec);
    Assert.assertTrue(json.contains("\"type\":\"stream_range\""));
    Assert.assertTrue(json.contains("\"partitionFilters\""));
  }

  @Test
  public void testJsonSerdeWithNullFilters() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final StreamRangeShardSpec original = new StreamRangeShardSpec(0, 1, null);

    final StreamRangeShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), StreamRangeShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertTrue(deserialized.getPartitionFilters().isEmpty());
  }

  @Test
  public void testEmptyStringValue_isDistinctFromNull()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
    // An IS NULL query (domain = (-inf, "")) must NOT match a segment that only declares the empty string.
    Assert.assertFalse(s.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testNullValue_matchesIsNullQueryOnly()
  {
    // A null/missing value is declared as a null list element.
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, Collections.singletonList(null)));
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "")));
  }

  @Test
  public void testConcreteValueOnly_isPrunedForIsNullQuery()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testNullAndConcreteValues_matchBoth()
  {
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null)));
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testNullValue_jsonSerdeRoundTrip() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final StreamRangeShardSpec original =
        new StreamRangeShardSpec(0, 1, ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null)));

    final StreamRangeShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), StreamRangeShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertTrue(deserialized.getPartitionFilters().get(TENANT).contains(null));
    Assert.assertTrue(deserialized.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testEmptyAllowedList_prunesEverything()
  {
    // An empty allowed list means no values were observed for the dimension, so any constraining query is pruned.
    final StreamRangeShardSpec s = spec(ImmutableMap.of(TENANT, List.of()));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
  }
}
