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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.loading.CompositePartialLoadSpec;
import org.apache.druid.segment.loading.PartialClusterGroupLoadSpec;
import org.apache.druid.segment.loading.PartialLoadSpec;
import org.apache.druid.segment.loading.PartialProjectionLoadSpec;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link CompositePartialLoadMatcher}: the union of member selections, the veto when a member cannot match,
 * single-member passthrough, and fingerprint stability.
 */
class CompositePartialLoadMatcherTest
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

  @Test
  void testConstructorRejectsNullMatchers()
  {
    org.apache.druid.error.DruidExceptionAssertions.assertMatches(
        Assertions.assertThrows(DruidException.class, () -> new CompositePartialLoadMatcher(null)),
        DruidExceptionMatcher.invalidInput().expectMessageContains("matchers must not be null or empty")
    );
  }

  @Test
  void testConstructorRejectsEmptyMatchers()
  {
    org.apache.druid.error.DruidExceptionAssertions.assertMatches(
        Assertions.assertThrows(DruidException.class, () -> new CompositePartialLoadMatcher(List.of())),
        DruidExceptionMatcher.invalidInput().expectMessageContains("matchers must not be null or empty")
    );
  }

  @Test
  void testConstructorRejectsNullMember()
  {
    org.apache.druid.error.DruidExceptionAssertions.assertMatches(
        Assertions.assertThrows(
            DruidException.class,
            () -> new CompositePartialLoadMatcher(Arrays.asList(exactProjection("p"), null))
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains("matchers[1] must not be null")
    );
  }

  @Test
  void testUnionsAcrossSchemes()
  {
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly", "user_daily"));
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        globClusterGroup(Map.of("tenant", "acme"))
    ));

    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(CompositePartialLoadSpec.TYPE, result.wrappedLoadSpec().get("type"));
    Assertions.assertEquals(BASE_LOAD_SPEC, result.wrappedLoadSpec().get("delegate"));
    Assertions.assertEquals(
        List.of(
            Map.of(
                "type", PartialProjectionLoadSpec.TYPE,
                "projections", List.of("user_hourly"),
                "fingerprint", exactProjection("user_hourly").match(segment, BASE_LOAD_SPEC).fingerprint()
            ),
            Map.of(
                "type", PartialClusterGroupLoadSpec.TYPE,
                "clusterGroupIndices", List.of(0, 1),
                "fingerprint", globClusterGroup(Map.of("tenant", "acme")).match(segment, BASE_LOAD_SPEC).fingerprint()
            )
        ),
        members(result)
    );
  }

  @Test
  void testUnionsTwoSameSchemeMembers()
  {
    final DataSegment segment = clusteredSegmentWithProjections(null);
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        globClusterGroup(Map.of("tenant", "acme", "region", "us-east-1")),
        globClusterGroup(Map.of("tenant", "globex", "region", "us-east-1"))
    ));

    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        List.of(List.of(0), List.of(2)),
        members(result).stream().map(m -> m.get("clusterGroupIndices")).toList()
    );
  }

  @Test
  void testUnionsPerBranchIncludeExcludePairs()
  {
    // A single globClusterGroup has one excludePatterns list, applied to every include. That makes an
    // include/exclude pair unexpressible as a unit whenever an exclude isn't qualified by its own branch's include:
    //   branch A — every acme region except us-west-2
    //   branch B — every globex region except us-east-1
    // Collapsing these into one matcher means both excludes apply to both includes, which over-excludes. Each branch
    // becomes its own composite member instead.
    final DataSegment segment = fourGroupSegment();
    final PartialLoadMatcher branchA = globClusterGroup(
        List.of(Map.of("tenant", "acme")),
        List.of(Map.of("region", "us-west-2"))
    );
    final PartialLoadMatcher branchB = globClusterGroup(
        List.of(Map.of("tenant", "globex")),
        List.of(Map.of("region", "us-east-1"))
    );

    final PartialLoadMatcher.MatchResult result =
        new CompositePartialLoadMatcher(List.of(branchA, branchB)).match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(
        // (acme, us-east-1) from branch A; (globex, us-west-2) from branch B
        List.of(List.of(0), List.of(3)),
        members(result).stream().map(m -> m.get("clusterGroupIndices")).toList()
    );

    // The collapsed single matcher is not equivalent: pooling the excludes wipes out every group.
    final PartialLoadMatcher.MatchResult collapsed = globClusterGroup(
        List.of(Map.of("tenant", "acme"), Map.of("tenant", "globex")),
        List.of(Map.of("region", "us-west-2"), Map.of("region", "us-east-1"))
    ).match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(collapsed);
    Assertions.assertEquals(List.of(), collapsed.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testMembersCarryNoDelegate()
  {
    // The composite carries the backend load spec exactly once, at the top level.
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly"));
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        globClusterGroup(Map.of("tenant", "acme"))
    ));
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    for (Map<String, Object> member : members(result)) {
      Assertions.assertFalse(
          member.containsKey(PartialLoadSpec.DELEGATE_FIELD),
          "member should not carry a delegate: " + member
      );
    }
  }

  @Test
  void testNullMemberVetoesWholeComposite()
  {
    // The cluster-group member cannot reason about a non-clustered segment. Skipping it would announce a segment
    // holding only its projections and none of its rows, so the composite goes opaque and the rule's
    // CannotMatchBehavior decides.
    final DataSegment segment = unclusteredSegmentWithProjections(List.of("user_hourly"));
    Assertions.assertNotNull(exactProjection("user_hourly").match(segment, BASE_LOAD_SPEC));
    Assertions.assertNull(globClusterGroup(Map.of("tenant", "acme")).match(segment, BASE_LOAD_SPEC));

    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        globClusterGroup(Map.of("tenant", "acme"))
    ));
    Assertions.assertNull(matcher.match(segment, BASE_LOAD_SPEC));
  }

  @Test
  void testUnknownMemberVetoesWholeComposite()
  {
    // A matcher type this Druid version doesn't recognize deserializes to UnknownPartialLoadMatcher, whose match()
    // returns null. The composite must escalate rather than silently narrow the load.
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly"));
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        new UnknownPartialLoadMatcher()
    ));
    Assertions.assertNull(matcher.match(segment, BASE_LOAD_SPEC));
  }

  @Test
  void testSingleMemberPassesThroughVerbatim()
  {
    // Wrapping a matcher in a one-element composite must not change the load spec or the fingerprint, so wrapping an
    // existing rule doesn't re-fingerprint every segment it covers.
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly"));
    final PartialLoadMatcher bare = exactProjection("user_hourly");
    final PartialLoadMatcher.MatchResult bareResult = bare.match(segment, BASE_LOAD_SPEC);
    final PartialLoadMatcher.MatchResult wrappedResult =
        new CompositePartialLoadMatcher(List.of(bare)).match(segment, BASE_LOAD_SPEC);

    Assertions.assertEquals(bareResult, wrappedResult);
    Assertions.assertEquals(PartialProjectionLoadSpec.TYPE, wrappedResult.wrappedLoadSpec().get("type"));
  }

  @Test
  void testAllEmptyMembersReportEmptyFingerprint()
  {
    // Every member resolved to an empty selection: the empty-load contract carries through composition.
    final DataSegment segment = clusteredSegmentWithProjections(null);
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        globClusterGroup(Map.of("tenant", "nobody")),
        globClusterGroup(Map.of("tenant", "nobody-else"))
    ));
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertEquals(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT, result.fingerprint());
    Assertions.assertEquals(
        List.of(List.of(), List.of()),
        members(result).stream().map(m -> m.get("clusterGroupIndices")).toList()
    );
  }

  @Test
  void testOneEmptyMemberDoesNotMakeCompositeEmpty()
  {
    final DataSegment segment = clusteredSegmentWithProjections(null);
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        globClusterGroup(Map.of("tenant", "acme")),
        globClusterGroup(Map.of("tenant", "nobody"))
    ));
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    Assertions.assertNotEquals(PartialLoadMatcher.EMPTY_LOAD_FINGERPRINT, result.fingerprint());
  }

  @Test
  void testFingerprintStableAcrossMatcherReordering()
  {
    // The resolved selection is a set union, so reordering the matchers must not thrash the cascade.
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly"));
    final PartialLoadMatcher projection = exactProjection("user_hourly");
    final PartialLoadMatcher clusterGroup = globClusterGroup(Map.of("tenant", "acme"));

    final String forward = new CompositePartialLoadMatcher(List.of(projection, clusterGroup))
        .match(segment, BASE_LOAD_SPEC).fingerprint();
    final String reversed = new CompositePartialLoadMatcher(List.of(clusterGroup, projection))
        .match(segment, BASE_LOAD_SPEC).fingerprint();
    Assertions.assertEquals(forward, reversed);
  }

  @Test
  void testFingerprintDiffersOnDifferentMemberContent()
  {
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly", "user_daily"));
    final String hourly = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        globClusterGroup(Map.of("tenant", "acme"))
    )).match(segment, BASE_LOAD_SPEC).fingerprint();
    final String daily = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_daily"),
        globClusterGroup(Map.of("tenant", "acme"))
    )).match(segment, BASE_LOAD_SPEC).fingerprint();
    Assertions.assertNotEquals(hourly, daily);
  }

  @Test
  void testFingerprintDiffersFromMemberFingerprints()
  {
    // Sanity: the composite mints its own fingerprint rather than reusing a member's, so a rule swap between a
    // composite and one of its members is detected.
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly"));
    final PartialLoadMatcher projection = exactProjection("user_hourly");
    final PartialLoadMatcher clusterGroup = globClusterGroup(Map.of("tenant", "acme"));
    final String composite = new CompositePartialLoadMatcher(List.of(projection, clusterGroup))
        .match(segment, BASE_LOAD_SPEC).fingerprint();
    Assertions.assertNotEquals(projection.match(segment, BASE_LOAD_SPEC).fingerprint(), composite);
    Assertions.assertNotEquals(clusterGroup.match(segment, BASE_LOAD_SPEC).fingerprint(), composite);
  }

  @Test
  void testNestedComposite()
  {
    final DataSegment segment = clusteredSegmentWithProjections(List.of("user_hourly"));
    final CompositePartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        new CompositePartialLoadMatcher(List.of(
            globClusterGroup(Map.of("tenant", "acme", "region", "us-east-1")),
            globClusterGroup(Map.of("tenant", "globex", "region", "us-east-1"))
        ))
    ));
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertNotNull(result);
    final List<Map<String, Object>> members = members(result);
    Assertions.assertEquals(PartialProjectionLoadSpec.TYPE, members.get(0).get("type"));
    Assertions.assertEquals(CompositePartialLoadSpec.TYPE, members.get(1).get("type"));
    // The nested composite's own members are likewise delegate-free.
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> nested = (List<Map<String, Object>>) members.get(1).get("members");
    for (Map<String, Object> m : nested) {
      Assertions.assertFalse(m.containsKey(PartialLoadSpec.DELEGATE_FIELD), "nested member has a delegate: " + m);
    }
  }

  @Test
  void testJsonRoundTrip() throws Exception
  {
    final PartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        globClusterGroup(Map.of("tenant", "acme"))
    ));
    final String json = mapper.writeValueAsString(matcher);
    final PartialLoadMatcher reread = mapper.readValue(json, PartialLoadMatcher.class);
    Assertions.assertInstanceOf(CompositePartialLoadMatcher.class, reread);
    Assertions.assertEquals(matcher, reread);
  }

  @Test
  void testJsonRoundTripNested() throws Exception
  {
    final PartialLoadMatcher matcher = new CompositePartialLoadMatcher(List.of(
        exactProjection("user_hourly"),
        new CompositePartialLoadMatcher(List.of(globClusterGroup(Map.of("tenant", "acme"))))
    ));
    final String json = mapper.writeValueAsString(matcher);
    Assertions.assertEquals(matcher, mapper.readValue(json, PartialLoadMatcher.class));
  }

  @Test
  void testEquals()
  {
    EqualsVerifier.forClass(CompositePartialLoadMatcher.class).usingGetClass().verify();
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> members(PartialLoadMatcher.MatchResult result)
  {
    return (List<Map<String, Object>>) result.wrappedLoadSpec().get("members");
  }

  private static PartialLoadMatcher exactProjection(String name)
  {
    return new ExactProjectionPartialLoadMatcher(List.of(name));
  }

  private static PartialLoadMatcher globClusterGroup(Map<String, String> pattern)
  {
    return new WildcardClusterGroupPartialLoadMatcher(List.of(pattern), null);
  }

  private static PartialLoadMatcher globClusterGroup(
      List<Map<String, String>> patterns,
      List<Map<String, String>> excludePatterns
  )
  {
    return new WildcardClusterGroupPartialLoadMatcher(patterns, excludePatterns);
  }

  private static RowSignature tenantRegion()
  {
    return RowSignature.builder()
                       .add("tenant", ColumnType.STRING)
                       .add("region", ColumnType.STRING)
                       .build();
  }

  /** A 3-group fixture: (acme, us-east-1), (acme, us-west-2), (globex, us-east-1). */
  private static DataSegment clusteredSegmentWithProjections(@Nullable List<String> projections)
  {
    final DataSegment.Builder builder = baseBuilder()
        .clusterGroups(new ClusterGroupTuples(
            tenantRegion(),
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "us-east-1")
            )
        ));
    if (projections != null) {
      builder.projections(projections);
    }
    return builder.build();
  }

  /**
   * A 4-group fixture spanning both tenants in both regions — (acme, us-east-1), (acme, us-west-2),
   * (globex, us-east-1), (globex, us-west-2) — so a region-only exclude scoped to one tenant's branch has something
   * to over-exclude in the other's if the two branches are pooled into a single matcher.
   */
  private static DataSegment fourGroupSegment()
  {
    return baseBuilder()
        .clusterGroups(new ClusterGroupTuples(
            tenantRegion(),
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "us-east-1"),
                List.of("globex", "us-west-2")
            )
        ))
        .build();
  }

  private static DataSegment unclusteredSegmentWithProjections(List<String> projections)
  {
    return baseBuilder().projections(projections).build();
  }

  private static DataSegment.Builder baseBuilder()
  {
    final NumberedShardSpec shardSpec = new NumberedShardSpec(0, 1);
    return DataSegment.builder(SegmentId.of("ds", Intervals.of("2026-01-01/2026-01-02"), "v", shardSpec))
                      .shardSpec(shardSpec)
                      .loadSpec(BASE_LOAD_SPEC)
                      .size(0);
  }
}
