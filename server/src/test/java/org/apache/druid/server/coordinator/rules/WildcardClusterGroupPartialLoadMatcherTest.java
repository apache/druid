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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
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
    return DataSegment.builder(SegmentId.of(
        "ds",
        Intervals.of("2026-01-01/2026-01-02"),
        "v",
        new NumberedShardSpec(0, 1)
    )).loadSpec(BASE_LOAD_SPEC).size(0).clusterGroups(groups).build();
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
    Assertions.assertNull(matcher.match(segment, BASE_LOAD_SPEC));
  }

  @Test
  void testUnknownColumnInPatternNoMatch()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("not_a_clustering_column", "anything")),
        null
    );
    Assertions.assertNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
  }

  @Test
  void testNoMatchReturnsNull()
  {
    final WildcardClusterGroupPartialLoadMatcher matcher = new WildcardClusterGroupPartialLoadMatcher(
        List.of(Map.of("tenant", "nobody")),
        null
    );
    Assertions.assertNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
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
}
