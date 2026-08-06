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
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimensionValueSetShardSpecTest
{
  private static final String TENANT = "tenant";
  private static final String CODE = "code";

  private static DimensionValueSetShardSpec spec(Map<String, List<String>> filters)
  {
    return new DimensionValueSetShardSpec(0, 1, filters);
  }

  private static DimensionValueSetShardSpec spec(
      Map<String, List<String>> filters,
      Map<String, ColumnType> types
  )
  {
    return new DimensionValueSetShardSpec(0, 1, filters, types);
  }

  private static TypedValueSet longValues(String... values)
  {
    final Set<String> set = new HashSet<>(Arrays.asList(values));
    return new TypedValueSet(set, ColumnType.LONG);
  }

  private static Map<String, TypedValueSet> longValueDomain(String dimension, String... values)
  {
    return ImmutableMap.of(dimension, longValues(values));
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
    final DimensionValueSetShardSpec s = spec(Collections.emptyMap());
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertTrue(s.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void testSingleFilter_matchingValue_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
  }

  @Test
  public void testSingleFilter_nonMatchingValue_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testSingleFilter_domainHasMultipleValues_matchIncluded_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a", "tenant_b")));
  }

  @Test
  public void testSingleFilter_domainHasMultipleValues_noMatch_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b", "tenant_c")));
  }

  @Test
  public void testMultipleAllowedValues_matchOne_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testMultipleAllowedValues_noMatch_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_c")));
  }

  @Test
  public void testDeclaredDimension_notInQueryDomain_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(Collections.emptyMap()));
  }

  @Test
  public void testDeclaredDimension_queryFiltersOnOtherDim_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertTrue(s.possibleInDomain(domain("region", "us-west")));
  }

  @Test
  public void testRangeFilter_onDeclaredDimension_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    // A range predicate (e.g. TENANT BETWEEN 'a' AND 'z') cannot be pruned against declared point values.
    Assert.assertTrue(s.possibleInDomain(rangeFilter(TENANT, "a", "z")));
  }

  @Test
  public void testMultipleDimensions_allMatch_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
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
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
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
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
        TENANT, List.of("tenant_a"),
        "region", List.of("us-west")
    ));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testGetDomainDimensions_returnsFilterKeys()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(
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
    Assert.assertEquals(ShardSpec.Type.DIM_VALUE_SET, spec(Collections.emptyMap()).getType());
  }

  private static ObjectMapper newMapper()
  {
    return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Test
  public void testJsonSerdeRoundTrip() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original = new DimensionValueSetShardSpec(
        3,
        8,
        ImmutableMap.of(TENANT, List.of("tenant_a", "tenant_b"), "region", List.of("us-west"))
    );

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), DimensionValueSetShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertEquals(original.getPartitionDimensionValues(), deserialized.getPartitionDimensionValues());
  }

  @Test
  public void testJsonSerdeContainsType() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec spec = new DimensionValueSetShardSpec(0, 1, ImmutableMap.of(TENANT, List.of("tenant_a")));
    final String json = mapper.writeValueAsString(spec);
    Assert.assertTrue(json.contains("\"type\":\"dim_value_set\""));
    Assert.assertTrue(json.contains("\"partitionDimensionValues\""));
  }

  @Test
  public void testJsonSerdeWithNullFilters() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original = new DimensionValueSetShardSpec(0, 1, null);

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), DimensionValueSetShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertTrue(deserialized.getPartitionDimensionValues().isEmpty());
  }

  @Test
  public void testEmptyStringValue_isDistinctFromNull()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("")));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
    // An IS NULL query (domain = (-inf, "")) must NOT match a segment that only declares the empty string.
    Assert.assertFalse(s.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testNullValue_matchesIsNullQueryOnly()
  {
    // A null/missing value is declared as a null list element.
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, Collections.singletonList(null)));
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "")));
  }

  @Test
  public void testConcreteValueOnly_isPrunedForIsNullQuery()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of("tenant_a")));
    Assert.assertFalse(s.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testNullAndConcreteValues_matchBoth()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null)));
    Assert.assertTrue(s.possibleInDomain(nullDomain(TENANT)));
    Assert.assertTrue(s.possibleInDomain(domain(TENANT, "tenant_a")));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_b")));
  }

  @Test
  public void testNullValue_jsonSerdeRoundTrip() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original =
        new DimensionValueSetShardSpec(0, 1, ImmutableMap.of(TENANT, Arrays.asList("tenant_a", null)));

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(mapper.writeValueAsString(original), DimensionValueSetShardSpec.class);

    Assert.assertEquals(original, deserialized);
    Assert.assertTrue(deserialized.getPartitionDimensionValues().get(TENANT).contains(null));
    Assert.assertTrue(deserialized.possibleInDomain(nullDomain(TENANT)));
  }

  @Test
  public void testEmptyAllowedList_prunesEverything()
  {
    // An empty allowed list means no values were observed for the dimension, so any constraining query is pruned.
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(TENANT, List.of()));
    Assert.assertFalse(s.possibleInDomain(domain(TENANT, "tenant_a")));
  }

  @Test
  public void testStringDomain_skipsTypeStampedDimension()
  {
    // A LONG-stamped dim holds canonicalized values ("1"); a non-canonical selector like code = "00001" matches
    // the indexed LONG via coercion, so the literal string range must not prune this segment.
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    Assert.assertTrue(s.possibleInDomain(domain(CODE, "00001")));
  }

  @Test
  public void testValueDomain_noFilters_alwaysTrue()
  {
    final DimensionValueSetShardSpec s = spec(Collections.emptyMap(), Collections.emptyMap());
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "1")));
    Assert.assertTrue(s.possibleInValueDomain(Collections.emptyMap()));
  }

  @Test
  public void testValueDomain_longMatch_returnsTrue()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1", "2")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "1")));
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "2")));
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "3", "2")));
  }

  @Test
  public void testValueDomain_longNoMatch_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1", "2")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    Assert.assertFalse(s.possibleInValueDomain(longValueDomain(CODE, "3")));
    Assert.assertFalse(s.possibleInValueDomain(longValueDomain(CODE, "999", "1000")));
  }

  @Test
  public void testValueDomain_noStampedType_cannotPrune()
  {
    // Values stamped but no type recorded (legacy/schemaless): must NOT prune without a matching type.
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(CODE, List.of("1", "2")));
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "3")));
  }

  @Test
  public void testValueDomain_typeMismatch_cannotPrune()
  {
    // A LONG filter must not prune a DOUBLE-stamped dim: cross-type stringification is unsafe (data-loss guard).
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1.0", "2.0")),
        ImmutableMap.of(CODE, ColumnType.DOUBLE)
    );
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "3")));
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "1")));
  }

  @Test
  public void testValueDomain_dimensionNotDeclared_cannotPrune()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(TENANT, List.of("tenant_a")),
        ImmutableMap.of(TENANT, ColumnType.STRING)
    );
    // Shard declares no values for the queried dimension → cannot prune.
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "3")));
  }

  @Test
  public void testValueDomain_multipleDimensions_oneMismatches_returnsFalse()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1"), "other", List.of("10")),
        ImmutableMap.of(CODE, ColumnType.LONG, "other", ColumnType.LONG)
    );
    Assert.assertTrue(s.possibleInValueDomain(ImmutableMap.of(
        CODE, longValues("1"),
        "other", longValues("10")
    )));
    Assert.assertFalse(s.possibleInValueDomain(ImmutableMap.of(
        CODE, longValues("1"),
        "other", longValues("999")
    )));
  }

  @Test
  public void testValueDomain_nullValueMembership()
  {
    // A null match value (e.g. IN (NULL)) must match a shard that declares a null value.
    final DimensionValueSetShardSpec withNull = spec(
        ImmutableMap.of(CODE, Arrays.asList("1", null)),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    final Set<String> nullSet = new HashSet<>();
    nullSet.add(null);
    Assert.assertTrue(withNull.possibleInValueDomain(ImmutableMap.of(CODE, new TypedValueSet(nullSet, ColumnType.LONG))));

    // A shard with only concrete values is pruned for a null-only match.
    final DimensionValueSetShardSpec concreteOnly = spec(
        ImmutableMap.of(CODE, List.of("1")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    Assert.assertFalse(concreteOnly.possibleInValueDomain(ImmutableMap.of(CODE, new TypedValueSet(nullSet, ColumnType.LONG))));
  }

  @Test
  public void testValueDomain_emptyAllowedList_prunes()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of()),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    Assert.assertFalse(s.possibleInValueDomain(longValueDomain(CODE, "1")));
  }

  @Test
  public void testWithPartitionNum_preservesTypes()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    final DimensionValueSetShardSpec renumbered = (DimensionValueSetShardSpec) s.withPartitionNum(5);
    Assert.assertEquals(5, renumbered.getPartitionNum());
    Assert.assertEquals(ImmutableMap.of(CODE, ColumnType.LONG), renumbered.getDimensionColumnTypes());
    Assert.assertFalse(renumbered.possibleInValueDomain(longValueDomain(CODE, "999")));
  }

  @Test
  public void testWithCorePartitions_preservesTypes()
  {
    final DimensionValueSetShardSpec s = spec(
        ImmutableMap.of(CODE, List.of("1")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    final DimensionValueSetShardSpec recored = (DimensionValueSetShardSpec) s.withCorePartitions(9);
    Assert.assertEquals(9, recored.getNumCorePartitions());
    Assert.assertEquals(ImmutableMap.of(CODE, ColumnType.LONG), recored.getDimensionColumnTypes());
  }

  @Test
  public void testEquals_distinguishesByType()
  {
    final DimensionValueSetShardSpec withLong = spec(
        ImmutableMap.of(CODE, List.of("1")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    final DimensionValueSetShardSpec withoutType = spec(ImmutableMap.of(CODE, List.of("1")));
    Assert.assertNotEquals(withLong, withoutType);
    Assert.assertEquals(withLong, spec(ImmutableMap.of(CODE, List.of("1")), ImmutableMap.of(CODE, ColumnType.LONG)));
  }

  @Test
  public void testThreeArgConstructor_hasEmptyTypes()
  {
    final DimensionValueSetShardSpec s = spec(ImmutableMap.of(CODE, List.of("1")));
    Assert.assertTrue(s.getDimensionColumnTypes().isEmpty());
    // With no stamped type, the value channel cannot prune.
    Assert.assertTrue(s.possibleInValueDomain(longValueDomain(CODE, "999")));
  }

  @Test
  public void testJsonSerde_withColumnTypes_roundTrips() throws Exception
  {
    final ObjectMapper mapper = newMapper();
    final DimensionValueSetShardSpec original = new DimensionValueSetShardSpec(
        3,
        8,
        ImmutableMap.of(CODE, List.of("1", "2")),
        ImmutableMap.of(CODE, ColumnType.LONG)
    );
    final String json = mapper.writeValueAsString(original);
    Assert.assertTrue(json.contains("\"dimensionColumnTypes\""));

    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(json, DimensionValueSetShardSpec.class);
    Assert.assertEquals(original, deserialized);
    Assert.assertEquals(ImmutableMap.of(CODE, ColumnType.LONG), deserialized.getDimensionColumnTypes());
    Assert.assertFalse(deserialized.possibleInValueDomain(longValueDomain(CODE, "3")));
  }

  @Test
  public void testJsonSerde_legacyWithoutColumnTypes_deserializes() throws Exception
  {
    // Back-compat: JSON written before dimensionColumnTypes existed deserializes to an empty map and does not prune
    // via the typed channel.
    final ObjectMapper mapper = newMapper();
    final String legacyJson =
        "{\"type\":\"dim_value_set\",\"partitionNum\":0,\"partitions\":1,"
        + "\"partitionDimensionValues\":{\"code\":[\"1\",\"2\"]}}";
    final DimensionValueSetShardSpec deserialized =
        mapper.readValue(legacyJson, DimensionValueSetShardSpec.class);
    Assert.assertTrue(deserialized.getDimensionColumnTypes().isEmpty());
    Assert.assertEquals(ImmutableMap.of(CODE, List.of("1", "2")), deserialized.getPartitionDimensionValues());
    Assert.assertTrue(deserialized.possibleInValueDomain(longValueDomain(CODE, "3")));
  }
}
