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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringDictionaries;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.ProjectionSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.projections.TableProjectionSchema;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class PartialClusterGroupLoadSpecTest
{
  private static final Map<String, Object> DELEGATE = ImmutableMap.of(
      "type", "stub",
      "path", "/var/druid/segments/foo"
  );
  private static final String FINGERPRINT = "v1:abcdef0123456789";

  private static ObjectMapper configuredMapper()
  {
    final ObjectMapper m = new DefaultObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.registerSubtypes(PartialClusterGroupLoadSpec.class, StubLoadSpec.class);
    m.registerModule(module);
    m.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, m));
    return m;
  }

  private final ObjectMapper jsonMapper = configuredMapper();

  @Test
  void testJsonRoundTrip() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0, 2, 5),
        FINGERPRINT,
        jsonMapper
    );
    String json = jsonMapper.writeValueAsString(spec);
    LoadSpec reread = jsonMapper.readValue(json, LoadSpec.class);
    Assertions.assertInstanceOf(PartialClusterGroupLoadSpec.class, reread);
    Assertions.assertEquals(spec, reread);
  }

  @Test
  void testWireFormHasPartialClusterGroupType() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0, 1),
        FINGERPRINT,
        jsonMapper
    );
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        new TypeReference<>()
        {
        }
    );
    Assertions.assertEquals("partialClusterGroup", wireForm.get("type"));
    Assertions.assertEquals(DELEGATE, wireForm.get("delegate"));
    Assertions.assertEquals(List.of(0, 1), wireForm.get("clusterGroupIndices"));
    Assertions.assertEquals(FINGERPRINT, wireForm.get("fingerprint"));
  }

  @Test
  void testLoadSegmentDelegatesToInner() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
        FINGERPRINT,
        jsonMapper
    );
    StubLoadSpec.LOAD_CALLS.set(0);
    LoadSpec.LoadSpecResult result = spec.loadSegment(new File("/tmp/dest"));
    Assertions.assertEquals(1, StubLoadSpec.LOAD_CALLS.get());
    Assertions.assertEquals(42L, result.getSize());
  }

  @Test
  void testOpenRangeReaderDelegatesToInner() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
        FINGERPRINT,
        jsonMapper
    );
    StubLoadSpec.RANGE_CALLS.set(0);
    SegmentRangeReader reader = spec.openRangeReader();
    Assertions.assertNotNull(reader);
    Assertions.assertEquals(1, StubLoadSpec.RANGE_CALLS.get());
  }

  @Test
  void testOpenRangeReaderReturnsNullWhenInnerDoesNotSupport() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        ImmutableMap.of("type", "stub", "path", "/", "supportsRange", false),
        List.of(0),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertNull(spec.openRangeReader());
  }

  @Test
  void testRejectsNullDelegate()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialClusterGroupLoadSpec(null, List.of(0), "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsNullClusterGroupIndices()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialClusterGroupLoadSpec(DELEGATE, null, "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsNullFingerprint()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialClusterGroupLoadSpec(DELEGATE, List.of(0), null, jsonMapper)
    );
  }

  @Test
  void testGetSelectedBundleNamesResolvesIndicesToBundleNames()
  {
    // Three cluster groups in the metadata; pick groups 0 and 2. Each index maps to the group's
    // clusteringValueIds → bundle name.
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(
            new TableClusterGroupSpec(List.of(0), 10),
            new TableClusterGroupSpec(List.of(1), 20),
            new TableClusterGroupSpec(List.of(2), 30)
        )
    );
    final DataSegment segment = clusteredSegment(
        List.of(List.of("acme"), List.of("globex"), List.of("initech"))
    );
    final PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0, 2),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(
        List.of(
            Projections.getClusterGroupBundleName(List.of(0)),
            Projections.getClusterGroupBundleName(List.of(2))
        ),
        spec.getSelectedBundleNames(segment, metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesEmptyIndicesReturnsEmpty()
  {
    // Sibling-empty: matcher applied but no positive selection. Caller pins only the metadata.
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(new TableClusterGroupSpec(List.of(0), 1))
    );
    final DataSegment segment = clusteredSegment(List.of(List.of("acme")));
    final PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(List.of(), spec.getSelectedBundleNames(segment, metadata));
  }

  @Test
  void testGetSelectedBundleNamesOutOfRangeIndexThrows()
  {
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(new TableClusterGroupSpec(List.of(0), 1))
    );
    final DataSegment segment = clusteredSegment(List.of(List.of("acme")));
    final PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(5),
        FINGERPRINT,
        jsonMapper
    );
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(segment, metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("Cluster-group index [5] is out of range"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testGetSelectedBundleNamesSizeMismatchThrows()
  {
    // Metadata has 2 groups, segment has 1 tuple — writer/reader contract violation; tripwire fires.
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(
            new TableClusterGroupSpec(List.of(0), 1),
            new TableClusterGroupSpec(List.of(1), 1)
        )
    );
    final DataSegment segment = clusteredSegment(List.of(List.of("acme")));
    final PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
        FINGERPRINT,
        jsonMapper
    );
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(segment, metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("Cluster-group count mismatch"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testGetSelectedBundleNamesNonClusteredBaseThrows()
  {
    // Base projection is a regular (non-clustered) table — partial cluster-group load is nonsensical.
    final ProjectionSchema baseSchema = new TableProjectionSchema(
        VirtualColumns.EMPTY,
        List.of(ColumnHolder.TIME_COLUMN_NAME, "tenant"),
        null,
        List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME))
    );
    final SegmentFileMetadata metadata = new SegmentFileMetadata(
        List.of(),
        Map.of(),
        null,
        null,
        List.of(new ProjectionMetadata(1, baseSchema)),
        null
    );
    final DataSegment segment = clusteredSegment(List.of(List.of("acme")));
    final PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
        FINGERPRINT,
        jsonMapper
    );
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(segment, metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("base projection is not clustered"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testGetSelectedBundleNamesNoProjectionsThrows()
  {
    final SegmentFileMetadata metadata = new SegmentFileMetadata(List.of(), Map.of(), null, null, null, null);
    final DataSegment segment = clusteredSegment(List.of(List.of("acme")));
    final PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
        FINGERPRINT,
        jsonMapper
    );
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(segment, metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("metadata has no projections"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  private static final RowSignature CLUSTERING_TENANT = RowSignature.builder()
                                                                    .add("tenant", ColumnType.STRING)
                                                                    .build();

  private static SegmentFileMetadata clusteredMetadata(List<TableClusterGroupSpec> groups)
  {
    final ClusteredValueGroupsBaseTableSchema baseSchema = new ClusteredValueGroupsBaseTableSchema(
        VirtualColumns.EMPTY,
        List.of(ColumnHolder.TIME_COLUMN_NAME, "tenant", "metric"),
        List.of(OrderBy.ascending("tenant"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
        CLUSTERING_TENANT,
        null,
        new ClusteringDictionaries(List.of("acme", "globex", "initech"), null, null, null),
        groups
    );
    return new SegmentFileMetadata(
        List.of(),
        Map.of(),
        null,
        null,
        List.of(new ProjectionMetadata(groups.stream().mapToInt(TableClusterGroupSpec::getNumRows).sum(), baseSchema)),
        null
    );
  }

  private static DataSegment clusteredSegment(List<List<Object>> tuples)
  {
    return DataSegment.builder(
                          SegmentId.of("ds", Intervals.ETERNITY, "v1", new NumberedShardSpec(0, 1))
                      )
                      .size(0)
                      .clusterGroups(new ClusterGroupTuples(CLUSTERING_TENANT, tuples))
                      .build();
  }

  /**
   * Stub LoadSpec used to verify delegation. Uses the same JSON "type"=="stub" key as the test {@link #DELEGATE}.
   */
  @JsonTypeName("stub")
  public static class StubLoadSpec implements LoadSpec
  {
    static final AtomicInteger LOAD_CALLS = new AtomicInteger(0);
    static final AtomicInteger RANGE_CALLS = new AtomicInteger(0);

    private final String path;
    private final boolean supportsRange;

    @JsonCreator
    public StubLoadSpec(
        @JsonProperty("path") String path,
        @JsonProperty("supportsRange") @Nullable Boolean supportsRange
    )
    {
      this.path = path;
      this.supportsRange = supportsRange == null || supportsRange;
    }

    @JsonProperty
    public String getPath()
    {
      return path;
    }

    @JsonProperty
    public boolean isSupportsRange()
    {
      return supportsRange;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir)
    {
      LOAD_CALLS.incrementAndGet();
      return new LoadSpecResult(42L);
    }

    @Override
    @Nullable
    public SegmentRangeReader openRangeReader()
    {
      if (!supportsRange) {
        return null;
      }
      RANGE_CALLS.incrementAndGet();
      return (filename, offset, length) -> new ByteArrayInputStream(new byte[0]);
    }
  }
}
