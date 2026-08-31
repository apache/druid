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
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringDictionaries;
import org.apache.druid.segment.projections.ProjectionMetadata;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class CompositePartialLoadSpecTest
{
  private static final Map<String, Object> DELEGATE = ImmutableMap.of(
      "type", "stub",
      "path", "/var/druid/segments/foo"
  );
  private static final String FINGERPRINT = "v1:abcdef0123456789";

  /**
   * A {@code partialProjection} member load spec, i.e. {@link PartialProjectionLoadSpec#wireForm} minus its
   * {@code delegate} — which is what {@code CompositePartialLoadMatcher} emits and what the composite injects into.
   */
  private static Map<String, Object> projectionMember(List<String> projections, String fingerprint)
  {
    return member(PartialProjectionLoadSpec.wireForm(DELEGATE, projections, fingerprint));
  }

  private static Map<String, Object> clusterGroupMember(List<Integer> indices, String fingerprint)
  {
    return member(PartialClusterGroupLoadSpec.wireForm(DELEGATE, indices, fingerprint));
  }

  private static Map<String, Object> member(Map<String, Object> wireForm)
  {
    final Map<String, Object> stripped = new HashMap<>(wireForm);
    stripped.remove(PartialLoadSpec.DELEGATE_FIELD);
    return stripped;
  }

  private static ObjectMapper configuredMapper()
  {
    final ObjectMapper m = new DefaultObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.registerSubtypes(
        CompositePartialLoadSpec.class,
        PartialProjectionLoadSpec.class,
        PartialClusterGroupLoadSpec.class,
        StubLoadSpec.class
    );
    m.registerModule(module);
    m.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, m));
    return m;
  }

  private final ObjectMapper jsonMapper = configuredMapper();

  @Test
  void testJsonRoundTrip() throws Exception
  {
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            clusterGroupMember(List.of(0, 2), "v1:bbbbbbbbbbbbbbbb")
        ),
        FINGERPRINT,
        jsonMapper
    );
    String json = jsonMapper.writeValueAsString(spec);
    LoadSpec reread = jsonMapper.readValue(json, LoadSpec.class);
    Assertions.assertInstanceOf(CompositePartialLoadSpec.class, reread);
    Assertions.assertEquals(spec, reread);
  }

  @Test
  void testWireFormHasPartialCompositeType() throws Exception
  {
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            clusterGroupMember(List.of(0), "v1:bbbbbbbbbbbbbbbb")
        ),
        FINGERPRINT,
        jsonMapper
    );
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        new TypeReference<>()
        {
        }
    );
    Assertions.assertEquals("partialComposite", wireForm.get("type"));
    Assertions.assertEquals(DELEGATE, wireForm.get("delegate"));
    Assertions.assertEquals(FINGERPRINT, wireForm.get("fingerprint"));
    Assertions.assertEquals(
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            clusterGroupMember(List.of(0), "v1:bbbbbbbbbbbbbbbb")
        ),
        wireForm.get("members")
    );
  }

  @Test
  void testMembersOnWireCarryNoDelegate() throws Exception
  {
    // The composite carries the backend load spec exactly once, at the top level.
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            clusterGroupMember(List.of(0), "v1:bbbbbbbbbbbbbbbb")
        ),
        FINGERPRINT,
        jsonMapper
    );
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        new TypeReference<>()
        {
        }
    );
    @SuppressWarnings("unchecked")
    final List<Map<String, Object>> members = (List<Map<String, Object>>) wireForm.get("members");
    for (Map<String, Object> m : members) {
      Assertions.assertFalse(m.containsKey("delegate"), "member should not carry a delegate: " + m);
    }
  }

  @Test
  void testDelegateIsInjectedIntoMembersOnMaterialization()
  {
    // Members omit the delegate on the wire, so the only way a member's own loadSegment can work is if the composite
    // spliced its delegate in. Materialization happens lazily inside getSelectedBundleNames.
    final SegmentFileMetadata metadata = projectionMetadata(List.of("user_hourly"));
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            projectionMember(List.of("user_hourly"), "v1:cccccccccccccccc")
        ),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(
        List.of("user_hourly"),
        spec.getSelectedBundleNames(unclusteredSegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesUnionsAcrossSchemes()
  {
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(
            new TableClusterGroupSpec(List.of(0), 10),
            new TableClusterGroupSpec(List.of(1), 20),
            new TableClusterGroupSpec(List.of(2), 30)
        ),
        List.of("user_hourly")
    );
    final DataSegment segment = clusteredSegment(
        List.of(List.of("acme"), List.of("globex"), List.of("initech"))
    );
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            clusterGroupMember(List.of(0, 2), "v1:bbbbbbbbbbbbbbbb")
        ),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(
        List.of(
            "user_hourly",
            Projections.getClusterGroupBundleName(List.of(0)),
            Projections.getClusterGroupBundleName(List.of(2))
        ),
        spec.getSelectedBundleNames(segment, metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesDedupesOverlappingMembers()
  {
    // Two same-scheme members whose selections overlap: the union drops the duplicate but keeps first-seen order.
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(
            new TableClusterGroupSpec(List.of(0), 10),
            new TableClusterGroupSpec(List.of(1), 20),
            new TableClusterGroupSpec(List.of(2), 30)
        ),
        null
    );
    final DataSegment segment = clusteredSegment(
        List.of(List.of("acme"), List.of("globex"), List.of("initech"))
    );
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            clusterGroupMember(List.of(0, 1), "v1:aaaaaaaaaaaaaaaa"),
            clusterGroupMember(List.of(1, 2), "v1:bbbbbbbbbbbbbbbb")
        ),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(
        List.of(
            Projections.getClusterGroupBundleName(List.of(0)),
            Projections.getClusterGroupBundleName(List.of(1)),
            Projections.getClusterGroupBundleName(List.of(2))
        ),
        spec.getSelectedBundleNames(segment, metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesAllEmptyMembersReturnsEmpty()
  {
    // Sibling-empty propagated through composition: every member selected nothing.
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(new TableClusterGroupSpec(List.of(0), 1)),
        null
    );
    final DataSegment segment = clusteredSegment(List.of(List.of("acme")));
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            clusterGroupMember(List.of(), "v1:partial-empty"),
            clusterGroupMember(List.of(), "v1:partial-empty")
        ),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(List.of(), spec.getSelectedBundleNames(segment, metadata));
  }

  @Test
  void testNestedCompositeInjectsDelegateRecursively()
  {
    final SegmentFileMetadata metadata = clusteredMetadata(
        List.of(
            new TableClusterGroupSpec(List.of(0), 10),
            new TableClusterGroupSpec(List.of(1), 20)
        ),
        List.of("user_hourly")
    );
    final DataSegment segment = clusteredSegment(List.of(List.of("acme"), List.of("globex")));
    final Map<String, Object> nested = member(
        CompositePartialLoadSpec.wireForm(
            DELEGATE,
            List.of(
                clusterGroupMember(List.of(0), "v1:aaaaaaaaaaaaaaaa"),
                clusterGroupMember(List.of(1), "v1:bbbbbbbbbbbbbbbb")
            ),
            "v1:dddddddddddddddd"
        )
    );
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(projectionMember(List.of("user_hourly"), "v1:cccccccccccccccc"), nested),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertEquals(
        List.of(
            "user_hourly",
            Projections.getClusterGroupBundleName(List.of(0)),
            Projections.getClusterGroupBundleName(List.of(1))
        ),
        spec.getSelectedBundleNames(segment, metadata)
    );
  }

  @Test
  void testMemberDefectPropagates()
  {
    // A member's own defensive tripwire is not swallowed by the union.
    final SegmentFileMetadata metadata = projectionMetadata(List.of("user_hourly"));
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(
            projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa"),
            projectionMember(List.of("nonexistent"), "v1:bbbbbbbbbbbbbbbb")
        ),
        FINGERPRINT,
        jsonMapper
    );
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(unclusteredSegment(), metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("does not contain projection[nonexistent]"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testLoadSegmentDelegatesToInner() throws Exception
  {
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa")),
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
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        DELEGATE,
        List.of(projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa")),
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
    CompositePartialLoadSpec spec = new CompositePartialLoadSpec(
        ImmutableMap.of("type", "stub", "path", "/", "supportsRange", false),
        List.of(projectionMember(List.of("user_hourly"), "v1:aaaaaaaaaaaaaaaa")),
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
        () -> new CompositePartialLoadSpec(
            null,
            List.of(projectionMember(List.of("a"), "v1:x")),
            "v1:x",
            jsonMapper
        )
    );
  }

  @Test
  void testRejectsNullFingerprint()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new CompositePartialLoadSpec(
            DELEGATE,
            List.of(projectionMember(List.of("a"), "v1:x")),
            null,
            jsonMapper
        )
    );
  }

  @Test
  void testRejectsNullMembers()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new CompositePartialLoadSpec(DELEGATE, null, "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsEmptyMembers()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new CompositePartialLoadSpec(DELEGATE, List.of(), "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsMemberCarryingDelegate()
  {
    // Unstripped member: the composite owns the delegate, so silently overwriting it could mask a real mismatch.
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> new CompositePartialLoadSpec(
            DELEGATE,
            List.of(PartialProjectionLoadSpec.wireForm(DELEGATE, List.of("a"), "v1:x")),
            "v1:x",
            jsonMapper
        )
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("must not carry its own [delegate]"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testRejectsMemberWithNonPartialType()
  {
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> new CompositePartialLoadSpec(
            DELEGATE,
            List.of(Map.of("type", "stub", "path", "/")),
            "v1:x",
            jsonMapper
        )
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("must be a partial load spec with a type starting with"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  private static final RowSignature CLUSTERING_TENANT = RowSignature.builder()
                                                                    .add("tenant", ColumnType.STRING)
                                                                    .build();

  private static SegmentFileMetadata clusteredMetadata(
      List<TableClusterGroupSpec> groups,
      @Nullable List<String> projections
  )
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
    final int numRows = groups.stream().mapToInt(TableClusterGroupSpec::getNumRows).sum();
    final List<ProjectionMetadata> projectionMetadata = new ArrayList<>();
    projectionMetadata.add(new ProjectionMetadata(numRows, baseSchema));
    if (projections != null) {
      for (String name : projections) {
        projectionMetadata.add(new ProjectionMetadata(numRows, projectionSchemaNamed(name)));
      }
    }
    return new SegmentFileMetadata(List.of(), Map.of(), null, null, null, projectionMetadata, null);
  }

  private static SegmentFileMetadata projectionMetadata(List<String> projections)
  {
    final List<ProjectionMetadata> projectionMetadata = new ArrayList<>();
    projectionMetadata.add(
        new ProjectionMetadata(
            100,
            new TableProjectionSchema(
                VirtualColumns.EMPTY,
                List.of(ColumnHolder.TIME_COLUMN_NAME, "tenant"),
                null,
                List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME))
            )
        )
    );
    for (String name : projections) {
      projectionMetadata.add(new ProjectionMetadata(10, projectionSchemaNamed(name)));
    }
    return new SegmentFileMetadata(List.of(), Map.of(), null, null, null, projectionMetadata, null);
  }

  private static AggregateProjectionSchema projectionSchemaNamed(String name)
  {
    return new AggregateProjectionSchema(
        name,
        null,
        null,
        VirtualColumns.EMPTY,
        List.of("tenant"),
        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
        List.of(OrderBy.ascending("tenant"))
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

  private static DataSegment unclusteredSegment()
  {
    return DataSegment.builder(
                          SegmentId.of("ds", Intervals.ETERNITY, "v1", new NumberedShardSpec(0, 1))
                      )
                      .size(0)
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
