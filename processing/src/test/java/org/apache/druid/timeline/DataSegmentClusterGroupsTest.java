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

package org.apache.druid.timeline;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Coverage for {@link DataSegment#getClusterGroups()} and the associated builder / interner plumbing.
 */
class DataSegmentClusterGroupsTest
{
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

  private static ClusterGroupTuples sampleGroups()
  {
    return new ClusterGroupTuples(
        tenantRegion(),
        List.of(List.of("acme", "us-east-1"), List.of("globex", "us-east-1"))
    );
  }

  private DataSegment.Builder baseBuilder()
  {
    return DataSegment.builder(SegmentId.of(
        "ds",
        Intervals.of("2026-01-01/2026-01-02"),
        "v",
        new NumberedShardSpec(0, 1)
    )).size(1L);
  }

  @Test
  void testNullByDefault()
  {
    final DataSegment segment = baseBuilder().build();
    Assertions.assertNull(segment.getClusterGroups());
  }

  @Test
  void testBuilderAttachesClusterGroups()
  {
    final ClusterGroupTuples groups = sampleGroups();
    final DataSegment segment = baseBuilder().clusterGroups(groups).build();
    Assertions.assertEquals(groups, segment.getClusterGroups());
  }

  @Test
  void testBuilderCopyPreservesClusterGroups()
  {
    final ClusterGroupTuples groups = sampleGroups();
    final DataSegment original = baseBuilder().clusterGroups(groups).build();
    final DataSegment copy = DataSegment.builder(original).build();
    Assertions.assertSame(original.getClusterGroups(), copy.getClusterGroups());
  }

  @Test
  void testWithClusterGroupsCanClearField()
  {
    final DataSegment original = baseBuilder().clusterGroups(sampleGroups()).build();
    final DataSegment cleared = original.withClusterGroups(null);
    Assertions.assertNull(cleared.getClusterGroups());
  }

  @Test
  void testJsonRoundTripWithClusterGroups() throws Exception
  {
    final DataSegment segment = baseBuilder().clusterGroups(sampleGroups()).build();
    final String json = mapper.writeValueAsString(segment);
    Assertions.assertTrue(json.contains("\"clusterGroups\""), () -> "expected clusterGroups in JSON: " + json);
    final DataSegment back = mapper.readValue(json, DataSegment.class);
    Assertions.assertEquals(segment.getClusterGroups(), back.getClusterGroups());
  }

  @Test
  void testJsonRoundTripOmitsNullClusterGroups() throws Exception
  {
    final DataSegment segment = baseBuilder().build();
    final String json = mapper.writeValueAsString(segment);
    Assertions.assertFalse(json.contains("clusterGroups"), () -> "did not expect clusterGroups in JSON: " + json);
    final DataSegment back = mapper.readValue(json, DataSegment.class);
    Assertions.assertNull(back.getClusterGroups());
  }

  @Test
  void testOlderJsonWithoutFieldDeserializes() throws Exception
  {
    // A representative pre-clusterGroups JSON shape; the field is simply absent. Older readers (and segments published
    // before this PR) don't carry it.
    final String legacyJson =
        "{\"dataSource\":\"ds\","
        + "\"interval\":\"2026-01-01T00:00:00.000Z/2026-01-02T00:00:00.000Z\","
        + "\"version\":\"v\","
        + "\"loadSpec\":{},"
        + "\"dimensions\":\"\",\"metrics\":\"\","
        + "\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":0,\"partitions\":1},"
        + "\"binaryVersion\":0,\"size\":1}";
    final DataSegment back = mapper.readValue(legacyJson, DataSegment.class);
    Assertions.assertNull(back.getClusterGroups());
  }

  @Test
  void testInternerReturnsSameInstanceAcrossSegments()
  {
    final DataSegment a = baseBuilder().clusterGroups(sampleGroups()).build();
    // sampleGroups() builds a fresh instance each call; the interner should dedupe identical content.
    final DataSegment b = DataSegment.builder(SegmentId.of(
        "ds-other",
        Intervals.of("2026-01-01/2026-01-02"),
        "v",
        new NumberedShardSpec(0, 1)
    )).size(1L).clusterGroups(sampleGroups()).build();
    Assertions.assertSame(a.getClusterGroups(), b.getClusterGroups());
  }

  @Test
  void testInternerDistinguishesDifferentContent()
  {
    final DataSegment a = baseBuilder().clusterGroups(sampleGroups()).build();
    final ClusterGroupTuples otherGroups = new ClusterGroupTuples(
        tenantRegion(),
        List.of(List.of("globex", "us-west-2"))
    );
    final DataSegment b = DataSegment.builder(SegmentId.of(
        "ds-other",
        Intervals.of("2026-01-01/2026-01-02"),
        "v",
        new NumberedShardSpec(0, 1)
    )).size(1L).clusterGroups(otherGroups).build();
    Assertions.assertNotSame(a.getClusterGroups(), b.getClusterGroups());
  }
}
