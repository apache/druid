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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;

public class SegmentDetailTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final DataSegment FULL_SEGMENT =
      DataSegment
          .builder(SegmentId.of("wiki", Intervals.of("2011/2012"), "v1", new NumberedShardSpec(3, 5)))
          .loadSpec(ImmutableMap.of("type", "local", "path", "/tmp/wiki"))
          .dimensions(Arrays.asList("dim1", "dim2"))
          .metrics(Arrays.asList("met1", "met2"))
          .projections(Arrays.asList("proj1", "proj2"))
          .clusterGroups(
              new ClusterGroupTuples(
                  RowSignature.builder().add("dim1", ColumnType.STRING).build(),
                  ImmutableList.of(ImmutableList.of("a"))
              )
          )
          .lastCompactionState(
              CompactionState.builder()
                             .partitionsSpec(new HashedPartitionsSpec(100, null, ImmutableList.of("dim1")))
                             .build()
          )
          .binaryVersion(9)
          .size(1234L)
          .totalRows(42)
          .indexingStateFingerprint("abcdef")
          .build();

  @BeforeAll
  public static void setUpClass()
  {
    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT);
    MAPPER.setInjectableValues(injectableValues);
  }

  @Test
  public void test_retainOnlyDetails_none()
  {
    final DataSegment retained = FULL_SEGMENT.retainOnlyDetails(SegmentDetail.none());

    // Mandatory fields survive.
    Assertions.assertEquals(FULL_SEGMENT.getId(), retained.getId());
    Assertions.assertEquals(FULL_SEGMENT.getShardSpec(), retained.getShardSpec());
    Assertions.assertEquals(FULL_SEGMENT.getBinaryVersion(), retained.getBinaryVersion());
    Assertions.assertEquals(FULL_SEGMENT.getSize(), retained.getSize());

    // Optional details do not.
    Assertions.assertNull(retained.getLoadSpec());
    Assertions.assertEquals(Collections.emptyList(), retained.getDimensions());
    Assertions.assertEquals(Collections.emptyList(), retained.getMetrics());
    Assertions.assertNull(retained.getProjections());
    Assertions.assertNull(retained.getClusterGroups());
    Assertions.assertNull(retained.getLastCompactionState());
    Assertions.assertNull(retained.getTotalRows());
    Assertions.assertNull(retained.getIndexingStateFingerprint());
  }

  @Test
  public void test_retainOnlyDetails_all()
  {
    final DataSegment retained = FULL_SEGMENT.retainOnlyDetails(SegmentDetail.all());
    assertAllFieldsEqual(FULL_SEGMENT, retained);
  }

  @Test
  public void test_retainOnlyDetails_null()
  {
    // Equivalent to "retain all".
    final DataSegment retained = FULL_SEGMENT.retainOnlyDetails(null);
    assertAllFieldsEqual(FULL_SEGMENT, retained);
  }

  @Test
  public void test_retainOnlyDetails_some()
  {
    final DataSegment retained =
        FULL_SEGMENT.retainOnlyDetails(EnumSet.of(SegmentDetail.LOAD_SPEC, SegmentDetail.ROW_COUNT));

    Assertions.assertEquals(FULL_SEGMENT.getLoadSpec(), retained.getLoadSpec());
    Assertions.assertEquals(FULL_SEGMENT.getTotalRows(), retained.getTotalRows());

    Assertions.assertEquals(Collections.emptyList(), retained.getDimensions());
    Assertions.assertEquals(Collections.emptyList(), retained.getMetrics());
    Assertions.assertNull(retained.getProjections());
    Assertions.assertNull(retained.getClusterGroups());
    Assertions.assertNull(retained.getLastCompactionState());
    Assertions.assertNull(retained.getIndexingStateFingerprint());
  }

  @Test
  public void test_all()
  {
    Assertions.assertEquals(SegmentDetail.values().length, SegmentDetail.all().size());
  }

  @Test
  public void test_none()
  {
    Assertions.assertEquals(0, SegmentDetail.none().size());
  }

  @Test
  public void test_fromNamesLenient()
  {
    Assertions.assertNull(SegmentDetail.fromNamesLenient(null));
    Assertions.assertEquals(SegmentDetail.none(), SegmentDetail.fromNamesLenient(Collections.emptyList()));
    Assertions.assertEquals(
        EnumSet.of(SegmentDetail.LOAD_SPEC, SegmentDetail.ROW_COUNT),
        SegmentDetail.fromNamesLenient(ImmutableList.of("loadSpec", "a_detail_from_the_future", "totalRows"))
    );
  }

  @Test
  public void test_serde() throws Exception
  {
    for (final SegmentDetail detail : SegmentDetail.values()) {
      final String json = MAPPER.writeValueAsString(detail);
      Assertions.assertEquals("\"" + detail + "\"", json);
      Assertions.assertEquals(detail, MAPPER.readValue(json, SegmentDetail.class));
    }

    Assertions.assertEquals(SegmentDetail.LOAD_SPEC, MAPPER.readValue("\"loadSpec\"", SegmentDetail.class));
    Assertions.assertThrows(Exception.class, () -> MAPPER.readValue("\"nonexistent\"", SegmentDetail.class));
  }

  @Test
  public void test_serde_ofDataSegment() throws Exception
  {
    final DataSegment retained = FULL_SEGMENT.retainOnlyDetails(EnumSet.of(SegmentDetail.LOAD_SPEC));
    final DataSegment deserialized = MAPPER.readValue(MAPPER.writeValueAsString(retained), DataSegment.class);
    assertAllFieldsEqual(retained, deserialized);
    Assertions.assertEquals(FULL_SEGMENT.getLoadSpec(), deserialized.getLoadSpec());
  }

  private static void assertAllFieldsEqual(final DataSegment expected, final DataSegment actual)
  {
    Assertions.assertEquals(expected.getId(), actual.getId(), "id");
    Assertions.assertEquals(expected.getShardSpec(), actual.getShardSpec(), "shardSpec");
    Assertions.assertEquals(expected.getBinaryVersion(), actual.getBinaryVersion(), "binaryVersion");
    Assertions.assertEquals(expected.getSize(), actual.getSize(), "size");
    Assertions.assertEquals(expected.getLoadSpec(), actual.getLoadSpec(), "loadSpec");
    Assertions.assertEquals(expected.getDimensions(), actual.getDimensions(), "dimensions");
    Assertions.assertEquals(expected.getMetrics(), actual.getMetrics(), "metrics");
    Assertions.assertEquals(expected.getProjections(), actual.getProjections(), "projections");
    Assertions.assertEquals(expected.getClusterGroups(), actual.getClusterGroups(), "clusterGroups");
    Assertions.assertEquals(expected.getLastCompactionState(), actual.getLastCompactionState(), "lastCompactionState");
    Assertions.assertEquals(expected.getTotalRows(), actual.getTotalRows(), "totalRows");
    Assertions.assertEquals(
        expected.getIndexingStateFingerprint(),
        actual.getIndexingStateFingerprint(),
        "indexingStateFingerprint"
    );
  }
}
