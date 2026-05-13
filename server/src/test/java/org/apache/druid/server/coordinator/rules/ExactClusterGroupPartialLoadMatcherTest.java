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

class ExactClusterGroupPartialLoadMatcherTest
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

  private static RowSignature tenantPriority()
  {
    return RowSignature.builder()
                       .add("tenant", ColumnType.STRING)
                       .add("priority", ColumnType.LONG)
                       .build();
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

  private static DataSegment threeGroupSegment()
  {
    return segmentWithGroups(new ClusterGroupTuples(
        tenantRegion(),
        Arrays.asList(
            Arrays.asList("acme", "us-east-1"),
            Arrays.asList("acme", "us-west-2"),
            Arrays.asList("globex", null)
        )
    ));
  }

  @Test
  void testConstructorRejectsNullTuples()
  {
    Assertions.assertThrows(DruidException.class, () -> new ExactClusterGroupPartialLoadMatcher(null));
  }

  @Test
  void testConstructorRejectsEmptyTuples()
  {
    Assertions.assertThrows(DruidException.class, () -> new ExactClusterGroupPartialLoadMatcher(List.of()));
  }

  @Test
  void testConstructorRejectsNullTupleEntry()
  {
    Assertions.assertThrows(
        DruidException.class,
        () -> new ExactClusterGroupPartialLoadMatcher(Arrays.asList(Arrays.asList("acme", "us-east-1"), null))
    );
  }

  @Test
  void testNonClusteredSegmentReturnsNull()
  {
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", "us-east-1"))
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
  void testSingleTupleMatch()
  {
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", "us-east-1"))
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testMultiTupleMatch()
  {
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", "us-east-1"), List.of("acme", "us-west-2"))
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0, 1), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testNullValueInRuleTupleMatchesNullInSegmentTuple()
  {
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        Arrays.asList(Arrays.asList("globex", null))
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(2), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testTupleLengthMismatchSkippedForSegment()
  {
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme"))   // segment has 2 clustering columns; this 1-col tuple won't match.
    );
    Assertions.assertNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
  }

  @Test
  void testCoercionAtMatchTime()
  {
    // Operator wrote Integer 5 (Jackson default for small ints); segment stores Long 5 (coerced at construction).
    final DataSegment segment = segmentWithGroups(new ClusterGroupTuples(
        tenantPriority(),
        List.of(List.of("acme", (Object) 5))
    ));
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", (Object) 5))   // Integer here; should coerce to Long for equality.
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testCoercionFailureSkipsTuple()
  {
    // String "acme" cannot be coerced to LONG for the priority column. The matcher silently skips that tuple
    // rather than throwing; other tuples in the matcher remain effective.
    final DataSegment segment = segmentWithGroups(new ClusterGroupTuples(
        tenantPriority(),
        List.of(List.of("acme", (Object) 5L), List.of("globex", (Object) 7L))
    ));
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(
            List.of("acme", "not_a_number"),   // bad type for priority — skipped.
            List.of("globex", (Object) 7)
        )
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(1), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testNoMatchReturnsNull()
  {
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("nobody", "us-east-1"))
    );
    Assertions.assertNull(matcher.match(threeGroupSegment(), BASE_LOAD_SPEC));
  }

  @Test
  void testIndicesAreSortedAndDeduped()
  {
    // Same tuple twice in the matcher config — deduped in the output.
    final ExactClusterGroupPartialLoadMatcher matcher = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", "us-east-1"), List.of("acme", "us-east-1"))
    );
    final PartialLoadMatcher.MatchResult result = matcher.match(threeGroupSegment(), BASE_LOAD_SPEC);
    Assertions.assertEquals(List.of(0), result.wrappedLoadSpec().get("clusterGroupIndices"));
  }

  @Test
  void testFingerprintStableAcrossTupleOrder()
  {
    final ExactClusterGroupPartialLoadMatcher a = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", "us-east-1"), List.of("acme", "us-west-2"))
    );
    final ExactClusterGroupPartialLoadMatcher b = new ExactClusterGroupPartialLoadMatcher(
        List.of(List.of("acme", "us-west-2"), List.of("acme", "us-east-1"))
    );
    Assertions.assertEquals(
        a.match(threeGroupSegment(), BASE_LOAD_SPEC).fingerprint(),
        b.match(threeGroupSegment(), BASE_LOAD_SPEC).fingerprint()
    );
  }

  @Test
  void testJsonRoundTrip() throws Exception
  {
    final ExactClusterGroupPartialLoadMatcher original = new ExactClusterGroupPartialLoadMatcher(
        Arrays.asList(
            Arrays.asList("acme", "us-east-1"),
            Arrays.asList("globex", null)
        )
    );
    final String json = mapper.writeValueAsString(original);
    final PartialLoadMatcher back = mapper.readValue(json, PartialLoadMatcher.class);
    Assertions.assertEquals(original, back);
  }
}
