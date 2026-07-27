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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class WildcardClusterGroupPartialLoadMatcherTest
{
  private static final Map<String, Object> BASE_LOAD_SPEC = Map.of("type", "local", "path", "/seg");

  private final ObjectMapper mapper = new DefaultObjectMapper();

  @BeforeEach
  void setUp()
  {
    final InjectableValues.Std injectables = new InjectableValues.Std();
    injectables.addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);
    mapper.setInjectableValues(injectables);
  }

  private static RowSignature tenantRegion()
  {
    return RowSignature.builder()
                       .add("tenant", ColumnType.STRING)
                       .add("region", ColumnType.STRING)
                       .build();
  }

  /** A 3-group fixture: (acme, us-east-1), (acme, us-west-2), (globex, us-east-1). */
  private static DataSegment threeGroupSegment()
  {
    return segmentWithGroups(new ClusterGroupTuples(
        tenantRegion(),
        List.of(
            List.of("acme", "us-east-1"),
            List.of("acme", "us-west-2"),
            List.of("globex", "us-east-1")
        )
    ));
  }

  private static DataSegment segmentWithGroups(ClusterGroupTuples groups)
  {
    return segmentWithGroupsAndShardSpec(groups, new NumberedShardSpec(0, 1));
  }

  private static DataSegment segmentWithGroupsAndShardSpec(ClusterGroupTuples groups, NumberedShardSpec shardSpec)
  {
    return DataSegment.builder(SegmentId.of(
        "ds",
        Intervals.of("2026-01-01/2026-01-02"),
        "v",
        shardSpec
    )).shardSpec(shardSpec).loadSpec(BASE_LOAD_SPEC).size(0).clusterGroups(groups).build();
  }

  @Test
  void testConstructorRejectsNullPatterns()
  {
    Assertions.assertThrows(DruidException.class, () -> new WildcardClusterGroupPartialLoadMatcher(null, null));
  }

  @Test
  void testConstructorRejectsEmptyPatterns()
  {
    Assertions.assertThrows(DruidException.class, () -> new WildcardClusterGroupPartialLoadMatcher(List.of(), null));
  }

  @Test
  void testConstructorRejectsEmptyPatternMap()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of()), null)
    );
  }

  @Test
  void testConstructorRejectsEmptyExcludePatternMap()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new WildcardClusterGroupPartialLoadMatcher(List.of(Map.of("tenant", "*")), List.of(Map.of()))
    );
  }

  @Test
  void testNonClusteredSegmentReturnsNull()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final DataSegment segment = DataSegment.builder(SegmentId.of(
        "ds",
        Intervals.of("2026-01-01/2026-01-02"),
        "v",
        new NumberedShardSpec(0, 1)
    )).loadSpec(BASE_LOAD_SPEC).size(0).build();
    Assertions.assertNull(matcher.match(segment, BASE_LOAD_SPEC));
  }

  @Test
  void testSingleColumnExactMatch()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(0, 1), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testSingleColumnGlob()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("region", "us-east-*")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(0, 2), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testMultiColumnPatternWithExactAndGlob()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "globex", "region", "us-east-*")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(2), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testOmittedColumnIsWildcard()
  {
    // No region constraint -> all acme rows match regardless of region.
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0, 1), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testExcludePatternRemovesGroups()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        List.of(Map.of("region", "us-west-2"))
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testStarMatchesAllIncludingNullValuesAtThatColumn()
  {
    final DataSegment segment = segmentWithGroups(new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(
            Arrays.asList("acme", "us-east-1"),
            Arrays.asList("acme", null),
            Arrays.asList(null, "us-east-1")
        )
    ));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("region", "*")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    // The "*" glob matches every value including null at the region position.
    Assertions.assertEquals(List.of(0, 1, 2), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testOmittedColumnMatchesNullAtThatPosition()
  {
    final DataSegment segment = segmentWithGroups(new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(
            Arrays.asList("acme", "us-east-1"),
            Arrays.asList("acme", null)
        )
    ));
    // No constraint on region; the null-region tuple is still picked up.
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0, 1), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testNonStarGlobDoesNotMatchNull()
  {
    final DataSegment segment = segmentWithGroups(new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(
            Arrays.asList("acme", "us-east-1"),
            Arrays.asList("acme", null)
        )
    ));
    // "us-*" must not match a null region (the only way to match null is "*" or omission).
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("region", "us-*")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testLiteralStringNullDoesNotMatchNullClusteringValue()
  {
    final DataSegment segment = segmentWithGroups(new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(Arrays.asList("acme", null))
    ));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("region", "null")),
        null
    );
    // Segment is clustered, no pattern matches → empty load.
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testUnknownColumnInPatternNoMatch()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("not_a_clustering_column", "anything")),
        null
    );
    // Pattern references a column the segment doesn't cluster on → matcher is incompatible with this segment's
    // clustering scheme → null (opaque), rule falls back to cannot-match handling.
    Assertions.assertNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
  }

  @Test
  void testIncompatibleClusteringReturnsNull()
  {
    // None of the matcher's pattern columns exist in the segment's clustering signature → the matcher is opaque to
    // this segment. The base class returns null (no empty load), letting the rule's cannot-match handling run.
    final RowSignature regionOnly = RowSignature.builder().add("region", ColumnType.STRING).build();
    final DataSegment regionClustered = segmentWithGroups(new ClusterGroupTuples(
        regionOnly,
        List.of(List.of("us-east-1"))
    ));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    Assertions.assertNull(matcher.match(regionClustered, BASE_LOAD_SPEC));
  }

  @Test
  void testCompatibleWhenAnyPatternResolves()
  {
    // Multi-pattern matcher where only one pattern's columns exist on the segment. At least one pattern resolves,
    // so the matcher is compatible and (when no tuple matches) returns the empty load for asymmetric
    // handling. Patterns that can't be resolved on this segment just don't contribute matches.
    final RowSignature tenantOnly = RowSignature.builder().add("tenant", ColumnType.STRING).build();
    final DataSegment tenantClustered = segmentWithGroups(new ClusterGroupTuples(
        tenantOnly,
        List.of(List.of("globex"))
    ));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        // pattern 1 resolves on tenant; pattern 2's "region" doesn't exist on this segment.
        List.of(Map.of("tenant", "acme"), Map.of("region", "us-east-*")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(tenantClustered, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testNoMatchReturnsEmptyWireForm()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "nobody")),
        null
    );
    // Segment is clustered, no pattern matches → empty load (asymmetric handling). The matcher still applies.
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals("partialClusterGroup", result.wrappedLoadSpec().get("type"));
    Assertions.assertEquals(List.of(), result.wrappedLoadSpec().get("clusterGroupIndices"));
    Assertions.assertEquals(BASE_LOAD_SPEC, result.wrappedLoadSpec().get("delegate"));
  }

  @Test
  void testIndicesAreSortedAndDeduped()
  {
    // Two patterns that both match the same group should not duplicate the index in the output.
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme"), Map.of("region", "us-east-1")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    // acme matches 0, 1; us-east-1 matches 0, 2 — union sorted = [0, 1, 2].
    Assertions.assertEquals(List.of(0, 1, 2), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testFingerprintStableAcrossPatternOrder()
  {
    final WildcardClusterGroupPartialLoadMatcher a = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme"), Map.of("region", "us-east-1")),
        null
    );
    final WildcardClusterGroupPartialLoadMatcher b = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("region", "us-east-1"), Map.of("tenant", "acme")),
        null
    );
    final PartialLoadMatcher.MatchResult ra = a.match(threeGroupSegment(), BASE_LOAD_SPEC);
    final PartialLoadMatcher.MatchResult rb = b.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(ra.fingerprint(), rb.fingerprint());
  }

  @Test
  void testJsonRoundTrip() throws Exception
  {
    final WildcardClusterGroupPartialLoadMatcher original = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme"), Map.of("tenant", "globex", "region", "us-east-*")),
        List.of(Map.of("region", "us-west-2"))
    );
    final String json = mapper.writeValueAsString(original);
    final PartialLoadMatcher back = mapper.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(original, back);
  }

  @Test
  void testOperatorVcResolvesToClusteringVcByEquivalence()
  {
    // Operator names their VC "queryLower" with lower(tenant); the segment's clustering VC is "tenant_lower" with
    // the same expression. The matcher should find equivalence and match the segment's tuples.
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("queryLower", "acme")),
        null,
        lowerTenantVcs("queryLower")
    );
    final DataSegment segment = vcClusteredSegment("acme", "globex");
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testOperatorVcWithoutEquivalenceIsNonMatching()
  {
    // Operator-VC computes upper(tenant); segment's clustering VC computes lower(tenant). Not equivalent → the
    // pattern is non-matching for this segment.
    final VirtualColumns operatorVcs = VirtualColumns.create(new ExpressionVirtualColumn(
        "queryUpper",
        "upper(tenant)",
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    ));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("queryUpper", "ACME")),
        null,
        operatorVcs
    );
    // The operator's VC has no equivalent on the segment → pattern unresolvable → matcher is incompatible with
    // this segment → null (opaque), rule falls back to cannot-match handling.
    Assertions.assertNull(matcher.match(vcClusteredSegment("acme"), BASE_LOAD_SPEC));
  }

  @Test
  void testOperatorVcShadowsClusteringColumnName()
  {
    // Operator declares a VC named "tenant" with an unrelated expression. The pattern key "tenant" resolves through
    // the operator's VC (operator-VC interpretation wins). Without an equivalent on the segment side, the pattern
    // is non-matching even though "tenant" happens to also be a clustering column name on a different segment.
    final VirtualColumns operatorVcs = VirtualColumns.create(new ExpressionVirtualColumn(
        "tenant",
        "lower(otherCol)",
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    ));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null,
        operatorVcs
    );
    // Segment that clusters directly on physical "tenant" (no segment VCs). The operator-VC shadowing rule means
    // we don't silently treat the pattern's "tenant" as the clustering column "tenant" — it's interpreted as the
    // operator-VC, which has no equivalent on the segment → unresolvable → matcher is incompatible with this
    // segment → null (opaque), rule falls back to cannot-match handling.
    Assertions.assertNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
  }

  @Test
  void testNoOperatorVcsKeepsDirectNameMatching()
  {
    // Sanity check: when no operator VCs are configured, the pattern key path falls through to direct clustering
    // column name resolution, unchanged from the pre-VC behavior.
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    Assertions.assertNotNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
  }

  @Test
  void testOperatorVcJsonRoundTrip() throws Exception
  {
    // Round-trip needs an injectable ExprMacroTable for ExpressionVirtualColumn deserialization.
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(ExprMacroTable.class, TestExprMacroTable.INSTANCE)
    );
    final WildcardClusterGroupPartialLoadMatcher original = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("queryLower", "acme")),
        null,
        lowerTenantVcs("queryLower")
    );
    final String json = mapper.writeValueAsString(original);
    Assertions.assertTrue(json.contains("\"virtualColumns\""), () -> "expected virtualColumns in JSON: " + json);
    final PartialLoadMatcher back = mapper.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(original, back);
  }

  @Test
  void testVirtualColumnsOmittedFromJsonWhenEmpty() throws Exception
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final String json = mapper.writeValueAsString(matcher);
    Assertions.assertFalse(json.contains("virtualColumns"), () -> "did not expect virtualColumns in JSON: " + json);
  }

  @Test
  void testNoMatchOnAppendedSegmentReturnsEmptyLoad()
  {
    // A clustered, compatible segment whose tuples match no pattern resolves to the empty load regardless of whether
    // it is a core or an appended (partitionNum >= numCorePartitions) partition. The empty result keeps the segment
    // announceable, so a fully-unmatched shard group can still be placed rather than dropped from every tier.
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantRegion(),
        List.of(List.of("acme", "us-east-1"))
    );
    final DataSegment appended = segmentWithGroupsAndShardSpec(groups, new NumberedShardSpec(2, 2));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "nobody")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(appended, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals("partialClusterGroup", result.wrappedLoadSpec().get("type"));
    Assertions.assertEquals(List.of(), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testNoMatchOnSegmentWithoutCorePartitionsReturnsEmptyLoad()
  {
    // numCorePartitions == 0 is treated like any other clustered, compatible segment: an unmatched resolve returns
    // the empty load so the segment can still be announced, not dropped.
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantRegion(),
        List.of(List.of("acme", "us-east-1"))
    );
    final DataSegment noCore = segmentWithGroupsAndShardSpec(groups, new NumberedShardSpec(0, 0));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "nobody")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(noCore, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testPositiveMatchOnAppendedSegmentStillReturnsWireForm()
  {
    // Positive match: the segment has the matcher's content. Even though it's an appended partition (no
    // completeness requirement), the operator still wants the matching content on this tier — dispatch the
    // partial load.
    final ClusterGroupTuples groups = new ClusterGroupTuples(
        tenantRegion(),
        List.of(List.of("acme", "us-east-1"))
    );
    final DataSegment appended = segmentWithGroupsAndShardSpec(groups, new NumberedShardSpec(2, 2));
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(appended, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testEmptyWireFormFingerprintIsStableAcrossInvocations()
  {
    // The empty-load (clustered segment, no patterns match) fingerprints must match across invocations so the
    // coordinator's reconciliation doesn't churn replicas between runs.
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "nobody")),
        null
    );
    final PartialLoadMatcher.MatchResult a = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    final PartialLoadMatcher.MatchResult b = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(a.fingerprint(), b.fingerprint());
    Assertions.assertTrue(
        a.fingerprint().startsWith(ClusterGroupPartialLoadMatcher.FINGERPRINT_VERSION + ":")
    );
  }

  @Test
  void testEmptyWireFormFingerprintDiffersFromPositiveMatch()
  {
    // A positive match and an empty load must produce different fingerprints; otherwise the coordinator can't
    // tell a real partial load from the empty stub for the same segment.
    final WildcardClusterGroupPartialLoadMatcher acme = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "acme")),
        null
    );
    final WildcardClusterGroupPartialLoadMatcher nobody = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "nobody")),
        null
    );
    final PartialLoadMatcher.MatchResult real = acme.match(threeGroupSegment(), BASE_LOAD_SPEC);
    final PartialLoadMatcher.MatchResult empty = nobody.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertNotNull(real);
    Assertions.assertNotNull(empty);
    Assertions.assertEquals(List.of(), empty.wrappedLoadSpec().get("clusterGroupIndices"));
    Assertions.assertNotEquals(real.fingerprint(), empty.fingerprint());
  }

  private static VirtualColumns lowerTenantVcs(String outputName)
  {
    return VirtualColumns.create(new ExpressionVirtualColumn(
        outputName,
        "lower(tenant)",
        ColumnType.STRING,
        TestExprMacroTable.INSTANCE
    ));
  }

  private static DataSegment vcClusteredSegment(String... lowerTenants)
  {
    final RowSignature clusteringColumns = RowSignature.builder().add("tenant_lower", ColumnType.STRING).build();
    final List<List<Object>> tuples = new ArrayList<>(lowerTenants.length);
    for (String t : lowerTenants) {
      tuples.add(Collections.singletonList(t));
    }
    return segmentWithGroups(new ClusterGroupTuples(clusteringColumns, lowerTenantVcs("tenant_lower"), tuples));
  }
}
